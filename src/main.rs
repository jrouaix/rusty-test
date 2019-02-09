extern crate csv;

use std::error::Error;
use std::io;
use std::process;
use std::boxed;
// use std::error;
// use std::fmt;

#[macro_use]
extern crate serde_derive;

// use serde_json::json;
// use serde::{Serialize};
// use serde_json::Result;

// use std::env;
// env::args()

// Output format (plain text, JSON, etc.) can now be configured by command line arguments. Integrate at least one new format.


fn main() {
    
    let input = io::stdin();
    //let handle = input;
    let handle = input.lock(); // change nothing after all

    // Build the CSV reader and iterate over each record.
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b';')
        .flexible(true)
        .from_reader(handle);

    let mut count = 0;
    let formater = JsonOutputFormater {};
    let line_separator = formater.get_line_separator();
    
    println!("{}", formater.get_output_begin());

    for result in rdr.deserialize() {
        count += 1;

        match result {
            Ok(content) => {
                let record: SourceLine = content;
                if let (Some(c), Some(d)) = (record.column_c, record.column_d) 
                {
                    let sum = c + d;
                    if sum > 100 {
                        let empty = String::default();
                        let a = record.column_a.as_ref().unwrap_or(&empty);
                        let b = record.column_b.as_ref().unwrap_or(&empty);
                        
                        let output = OkLineOutput {
                            line_number: count,
                            line_type: String::from("ok"),
                            concat_ab: format!("{}{}", a, b),
                            sum_cd: record.column_c.unwrap() + record.column_c.unwrap(),
                        };

                        let output = formater.format_ok_line(&count, &output);
                        
                        let mut jsonline = if count > 1 { String::from(line_separator) } else { String::default() }; 
                        jsonline.push_str(&output);
                        println!("{}", jsonline);
                    }
                }
            }
            Err(err) => {
                let output = ErrorLineOutput {
                    line_number: count,
                    line_type: String::from("error"),
                    error_message: format!("{:?}", err),
                };

                let output = formater.format_error_line(&count, &output);

                let mut jsonline = if count > 1 { String::from(", ") } else { String::from("") }; 
                jsonline.push_str(&output);
                println!("{}", jsonline);
            }
        }
    }

    println!("{}", formater.get_output_end());
}

// ***************************************************
//                      TARGET
// ***************************************************


#[derive(Debug, Serialize)]
struct OkLineOutput {
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
struct ErrorLineOutput {
    #[serde(rename = "lineNumber")]
    line_number: i32,
    #[serde(rename = "lineType")]
    line_type: String, // TODO : serialise to "type"
    #[serde(rename = "errorMessage")]
    error_message: String,
}


trait OutputFormater {
    
    fn format_ok_line(&self, line_number : &i32, line: &OkLineOutput) -> String;
    fn format_error_line(&self, line_number : &i32,  err: &ErrorLineOutput) -> String;

    fn get_line_separator(&self) -> &'static str;
    fn get_output_begin(&self) -> &'static str;
    fn get_output_end(&self) -> &'static str;
}

// Json



struct JsonOutputFormater 
{

}

impl OutputFormater for JsonOutputFormater{
    fn format_ok_line(&self, line_number : &i32, line: &OkLineOutput) -> String{
        serde_json::to_string(line)
            .unwrap_or_else(|e| panic!(e)) // should not happen
    }

    fn format_error_line(&self, line_number : &i32,  err: &ErrorLineOutput) -> String{
        serde_json::to_string(err)
            .unwrap_or_else(|e| panic!(e)) // should not happen
    }

    fn get_line_separator(&self) -> &'static str { "\n ," }
    fn get_output_begin(&self) -> &'static str { "[" }
    fn get_output_end(&self) -> &'static str { "]" }
}

// Text


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

    // iterator: Box<csv::DeserializeRecordsIter<'r, &'r mut R, SourceLine>>,
    // iterator:     csv::DeserializeRecordsIter<'r, &'r mut R, SourceLine>,

struct CsvSourceIterator<'r, R: io::Read>{
    iterator: csv::DeserializeRecordsIter<'r, R, SourceLine>,
}

impl<'r, R: io::Read> CsvSourceIterator<'r, R> {
    // fn new(rdr : R) -> CsvSourceIterator<'r, R> {
    //     let csv = csv::ReaderBuilder::new()
    //         .delimiter(b';')
    //         .flexible(true)
    //         .from_reader(rdr)
    //         .deserialize();
            
    //     CsvSourceIterator { iterator : csv }
    // }

    // fn new(rdr : R) -> CsvSourceIterator<'r, R> {
    //     // let test= Box::new(5);

    //     let mut csv1 = Box::new(csv::ReaderBuilder::new());
    //     let csv2 = Box::new(csv1.delimiter(b';'));
    //     let csv3 = Box::new(csv2.flexible(true));
    //     let mut csv4 = Box::new(csv3.from_reader(rdr));
    //     let csv5 = Box<T + 'r>::new(csv4.deserialize();
            
    //     CsvSourceIterator { iterator : csv5 }
    // }

    // fn new(rdr : &'r mut R) -> CsvSourceIterator<'r, R> {
    //     let mut csv1 = csv::ReaderBuilder::new();
    //     let csv2 = csv1.delimiter(b';');
    //     let csv3 = csv2.flexible(true);
    //     let mut csv4 = csv3.from_reader(rdr);
    //     let csv5 = csv4.deserialize();
        
    //     // let d  = ;
    //     // //let d = csv.deserialize();

    //     CsvSourceIterator { iterator : csv5 }
    // }
}


// impl<R: io::Read> Iterator for CsvSourceIterator<R>{
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
