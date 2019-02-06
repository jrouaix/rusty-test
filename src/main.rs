extern crate csv;

use std::error::Error;
use std::io;
use std::process;
use std::fmt;

#[macro_use]
extern crate serde_derive;

use serde_json::json;
use serde::{Serialize};
// use serde_json::Result;



/*

Replace the output from the previous step. Write a big JSON array of objects for the previous matched lines:
- Rows that can be processed correctly : { "lineNumber": <FILE_LINE_NUMBER>, "type": "ok", "concatAB": "<PREVIOUS_AB_CONCAT>", "sumCD": <PREVIOUS_CD_SUM> }
- Rows that can't be processed correctly : { "lineNumber": <FILE_LINE_NUMBER>, "type": "error", "errorMessage": <ERROR_MESSAGE> }

*/


#[derive(Debug, Deserialize)]
struct CsvLine {
    column: Option<String>,
    columnA: Option<String>, 
    columnB: Option<String>,
    columnC : Option<i64>,
    // column_c : String,
    columnD: Option<i64>,
    otherColumn: Option<String>
}

#[derive(Debug, Serialize)]
struct JsonOkLineOutput {
    lineNumber: i32,
    lineType: String, // TODO : serialise to "type"
    concatAB: String,
    sumCD: i64,
}

#[derive(Debug, Serialize)]
struct JsonErrorLineOutput {
    lineNumber: i32,
    lineType: String, // TODO : serialise to "type"
    errorMessage: String,
}


fn example() -> Result<(), Box<Error>> {
    let input = io::stdin();
    let handle = input;
    // let handle = input.lock(); // change nothing after all

    // Build the CSV reader and iterate over each record.
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b';')
        .flexible(true)
        .from_reader(handle);

    let mut count = 0;

    println!("[");

    for result in rdr.deserialize() {
        count += 1;

        match result {
            Err(err) => {
                let output = JsonErrorLineOutput {
                    lineNumber: count,
                    lineType: String::from("error"),
                    errorMessage: format!("{:?}", err),
                };

                let mut jsonline = if count > 1 { String::from(", ") } else { String::from("") }; 
                jsonline.push_str(&serde_json::to_string(&output)?);
                println!("{}", jsonline);
            }
            Ok(_) => {
                let record: CsvLine = result?;
                if let (Some(a), Some(b), Some(c), Some(d)) = (record.columnA, record.columnB, record.columnC, record.columnD) 
                {
                    let sum = c + d;
                    if sum > 100 {   
                        let output = JsonOkLineOutput {
                            lineNumber: count,
                            lineType: String::from("ok"),
                            concatAB: format!("{}{}", a, b),
                            sumCD: c+d,
                        };

                        let mut jsonline = if count > 1 { String::from(", ") } else { String::from("") }; 
                        jsonline.push_str(&serde_json::to_string(&output)?);
                        println!("{}", jsonline);
                    }
                }
            }
        }
    }

    println!("]");

    Ok(())
}

fn main() {
    if let Err(err) = example() {
        println!("error running example: {}", err);
        process::exit(1);
    }
}