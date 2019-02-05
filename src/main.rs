extern crate csv;

use std::error::Error;
use std::io;
use std::process;
#[macro_use]
extern crate serde_derive;


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

        match record.columnC {
            None    => continue,
            Some(c) => {
                match record.columnD {
                    None    => continue,
                    Some(d) => {
                        let sum = c + d;
                        if sum > 100 {
                            println!("{}{}", c, d);
                        }
                    }
                }
            },
        }
    }

    Ok(())
}

fn main() {
    if let Err(err) = example() {
        println!("error running example: {}", err);
        process::exit(1);
    }
}