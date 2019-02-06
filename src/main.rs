extern crate csv;

use std::error::Error;
use std::io;
use std::process;
// use std::fmt;

#[macro_use]
extern crate serde_derive;

// use serde_json::json;
// use serde::{Serialize};
// use serde_json::Result;



/*

Replace the output from the previous step. Write a big JSON array of objects for the previous matched lines:
- Rows that can be processed correctly : { "lineNumber": <FILE_LINE_NUMBER>, "type": "ok", "concatAB": "<PREVIOUS_AB_CONCAT>", "sumCD": <PREVIOUS_CD_SUM> }
- Rows that can't be processed correctly : { "lineNumber": <FILE_LINE_NUMBER>, "type": "error", "errorMessage": <ERROR_MESSAGE> }

*/


#[derive(Debug, Serialize)]
struct JsonOkLineOutput {
    #[serde(rename = "lineNumber")]
    line_number: i32,
    #[serde(rename = "lineType")]
    line_type: String, // TODO : serialise to "type"
    #[serde(rename = "concatAB")]
    concat_ab: String,
    #[serde(rename = "sumCD")]
    sum_cd: i64,
}

#[derive(Debug, Serialize)]
struct JsonErrorLineOutput {
    #[serde(rename = "lineNumber")]
    line_number: i32,
    #[serde(rename = "lineType")]
    line_type: String, // TODO : serialise to "type"
    #[serde(rename = "errorMessage")]
    error_message: String,
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
                    line_number: count,
                    line_type: String::from("error"),
                    error_message: format!("{:?}", err),
                };

                let mut jsonline = if count > 1 { String::from(", ") } else { String::from("") }; 
                jsonline.push_str(&serde_json::to_string(&output)?);
                println!("{}", jsonline);
            }
            Ok(_) => {
                let record: SourceLine = result?;
                if let (Some(a), Some(b), Some(c), Some(d)) = (record.column_a, record.column_b, record.column_c, record.column_d) 
                {
                    let sum = c + d;
                    if sum > 100 {
                        let output = JsonOkLineOutput {
                            line_number: count,
                            line_type: String::from("ok"),
                            concat_ab: format!("{}{}", a, b),
                            sum_cd: c+d,
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


// ***************************************************
//                      SOURCE
// ***************************************************

#[derive(Debug, Deserialize)]
struct SourceLine {
    column: Option<String>,
    #[serde(rename = "columnA")]
    column_a: Option<String>, 
    #[serde(rename = "columnB")]
    column_b: Option<String>,
    #[serde(rename = "columnC")]
    column_c : Option<i64>,
    #[serde(rename = "columnD")]
    column_d: Option<i64>,
    #[serde(rename = "otherColumn")]
    other_column: Option<String>
}

/*
struct CsvSource<'r, R: io::Read>{
    iterator: csv::DeserializeRecordsIter<'r, R, SourceLine>,
}

impl<'r, R: io::Read> CsvSource<'r, R> {
    fn new(rdr : &'r mut R) -> CsvSource<'r, R> {
        CsvSource { iterator : rdr.deserialize<SourceLine>() }

        // let deser:SourceLine =  rdr.deserialize();

        // return CsvSource { iterator: deser }; 
        // CsvSource { iterator : () }
        // Source { iterator : csv::DeserializeRecordsIter::new(rdr) }
    }
}
*/

// impl<R: io::Read> Iterator for Source<R>{
//     type Item = SourceLine;

//     fn next(&mut self) -> Option<Self::Item>{
//         let next = self.reader.next();
        
//         None
//         // reader
//     }
// }

// pub fn from_reader<R: io::Read>(&self, rdr: R) -> Reader<R> {
//         Reader::new(self, rdr)
//     }

// impl Iterator for Source {
//     // we will be counting with usize
//     type Item = SourceLine;

//     // next() is the only required method
//     fn next(&mut self) -> Option<usize> {
//         // Increment our count. This is why we started at zero.
//         self.count += 1;

//         // Check to see if we've finished counting or not.
//         if self.count < 6 {
//             Some(self.count)
//         } else {
//             None
//         }
//     }
// }

// ***************************************************
//                      SOURCE
// ***************************************************
