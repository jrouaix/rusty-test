extern crate csv;

use std::error::Error;
use std::io;
use std::process;
#[macro_use]
extern crate serde_derive;

/*

Replace the output from the previous step. Write a big JSON array of objects for the previous matched lines:
- Rows that can be processed correctly : { "lineNumber": <FILE_LINE_NUMBER>, "type": "ok", "concatAB": "<PREVIOUS_AB_CONCAT>", "sumCD": <PREVIOUS_CD_SUM> }
- Rows that can't be processed correctly : { "lineNumber": <FILE_LINE_NUMBER>, "type": "error", "errorMessage": <ERROR_MESSAGE> }

*/


#[derive(Debug, Deserialize)]
struct Record {
    column: Option<String>,
    columnA: Option<String>, 
    columnB: Option<String>,
    columnC : Option<i64>,
    // column_c : String,
    columnD: Option<i64>,
    otherColumn: Option<String>
}



fn example() -> Result<(), Box<Error>> {
    // Build the CSV reader and iterate over each record.
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b';')
        .flexible(true)
        .from_reader(io::stdin());

    for result in rdr.deserialize() {
        if let Err(_) = result { continue }
        let record: Record = result?;

        // println!("{:?}", record);
        if let (Some(a), Some(b), Some(c), Some(d)) = (record.columnA, record.columnB, record.columnC, record.columnD) 
        {
            let sum = c + d;
            if sum > 100 {
                println!("{}{}", a, b);
            }
        }

        // THIS MATCHING DOES NOT WORK :-/
        // match (record.columnA, record.columnB, record.columnC, record.columnD) {
        //     _    => continue,
        //     (Some(a), Some(b), Some(c), Some(d)) => {
        //         let sum = c + d;
        //         if sum > 100 {
        //             println!("{}{}", a, b);
        //         }
        //     },
        // }
    }

    Ok(())
}

fn main() {
    if let Err(err) = example() {
        println!("error running example: {}", err);
        process::exit(1);
    }
}