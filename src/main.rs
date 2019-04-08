#![feature(await_macro, futures_api, async_await)]

use std::io;

#[macro_use]
extern crate serde_derive;

use actix_web::{server, http, Query, /*HttpRequest, HttpResponse, Error, Body, Path,*/ Responder, middleware};

// use bytes::Bytes;
// use std::future::Future;
// use futures::stream::once;
// use actix_web_async_await::{await, compat};
// use std::time::{Instant, Duration};
// use tokio::timer::Delay;



fn main() {
    
    let arguments = clap::App::new("bz-test")
        .version(clap::crate_version!())
        .about("csv to whatever in rust")
        .author(clap::crate_authors!())
        .subcommand(clap::SubCommand::with_name("process").about("Run transformation from stdin or a file.")
            .arg(clap::Arg::with_name("file").short("f").takes_value(true))
            .arg(clap::Arg::with_name("out").short("o").takes_value(true).default_value("json").possible_values(&["json", "text"]))
        )
        .subcommand(clap::SubCommand::with_name("webserver").about("Run transformation through a rest API.")
            .arg(clap::Arg::with_name("port").short("p").takes_value(true).default_value("4242"))
        )
        .get_matches()
        ;

    match arguments.subcommand() {
        ("process", Some(command_matches)) =>{
            let formater = get_formater(command_matches.value_of("out").unwrap());
            let input = io::stdin();
            let mut output = io::stdout();
            process(formater, input, &mut output);
        },
        ("webserver", Some(webserver_matches)) =>{
            let port = webserver_matches.value_of("port").unwrap().parse::<i32>().unwrap();

            let sys = actix::System::new("example");  // <- create Actix system

            let address = format!("0.0.0.0:{}", port);
            server::new(|| actix_web::App::new()
                .middleware(middleware::Logger::default())
                .resource(
                    "/filter",
                    |r| r
                        .method(http::Method::GET)
                        .with(filter)
                        // .with(compat(filter))
                    ))
                .bind(&address)
                .expect(&format!("Can not bind to {}.", &address))
                .start()
                ;

            sys.run();
        },
        (_, None)   => println!("Use a subcommand : process or webservice. see help for more informations."), 
        _            => unreachable!(), 
    }
}

// TODO : https://github.com/actix/examples/blob/master/multipart/src/main.rs

#[derive(Deserialize, Debug)]
struct Info {
    #[serde(rename = "csvUri")]
    csv_uri: String,
    #[serde(default = "default_formater")]
    format: String,
}

fn default_formater() -> String { "json".to_string() }


// async fn filter(info: Query<Info>) -> Result<String, Error> {
//     // Wait 2s
//     await!(Delay::new(Instant::now() + Duration::from_secs(2)))?;

//     // Proceed with normal response
//     Ok(format!("Hello {}! id:{}", info.csv_uri, info.format))
// }


// https://docs.rs/actix-web/0.4.5/src/actix_web/fs.rs.html#166-231
// http://localhost:8000/12654/alzifg/index.html
fn filter(info: Query<Info>) -> impl Responder {

    let csv = reqwest::get(&info.csv_uri).unwrap();
    let formater = get_formater(&info.format);
    let mut buffer: Vec<u8> = vec!{};

    process(formater, csv, &mut buffer);

    let output = std::str::from_utf8(&buffer[..]).unwrap();
    output.to_string() 

    // return response;

   

    // await!(Delay::new(Instant::now() + Duration::from_secs(2)))?;



    // process(formater, csv, &mut response);
    // HttpResponse::Ok()
    //     .chunked()
    //     .body(Body::Streaming(Box::new(once(Ok(Bytes::from_static(b"data"))))))

    // return response;       
    // actix_web::result(Ok(HttpResponse::Ok()
    //           .content_type("text/html")
    //           .body(format!("Hello!"))))
    //        .responder()
}


fn process<R: io::Read, W: io::Write>(formater : Box<OutputFormater>, reader : R, writer : &mut W) {

    // Build the CSV reader and iterate over each record.
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b';')
        .flexible(true)
        .from_reader(reader);

    let mut count = 0;
    let line_separator = formater.get_line_separator();

    writeln!(writer, "{}", formater.get_output_begin()).expect("write error");

    for result in rdr.deserialize() {
        count += 1;

        match result {
            Ok(record) => {
                let record: SourceLine = record;
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

                        let output = formater.format_ok_line(&output);
                        write_line(writer, output, &count, line_separator);
                    }
                }
            }
            Err(err) => {
                let output = ErrorLineOutput {
                    line_number: count,
                    line_type: String::from("error"),
                    error_message: format!("{:?}", err),
                };

                let output = formater.format_error_line(&output);
                write_line(writer, output, &count, line_separator);
            }
        }
    }

    writeln!(writer, "{}", formater.get_output_end()).expect("write error");;
}

fn write_line<W: io::Write>(writer: &mut W, output: String, line_count: &i32, line_separator: &str)
{
    let mut output_line = if *line_count > 1 { String::from(line_separator) } else { String::default() };  
    output_line.push_str(&output);
    writeln!(writer, "{}",  output_line).expect("write error");        
}

// ***************************************************
//                      TARGET
// ***************************************************

fn get_formater(output_type_name: &str) -> Box<OutputFormater> 
{
    match output_type_name {
        "json" => Box::new(JsonOutputFormater{}),
        "text" => Box::new(TextOutputFormater{}),
        _ => unimplemented!(),
    }
}

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
    fn format_ok_line(&self, line: &OkLineOutput) -> String;
    fn format_error_line(&self, err: &ErrorLineOutput) -> String;

    fn get_line_separator(&self) -> &'static str;
    fn get_output_begin(&self) -> &'static str;
    fn get_output_end(&self) -> &'static str;
}

// enum OutFormater {
//     Json,
//     Text
// }

// impl FromStr for OutFormater {
//     type Err = ();

//     fn from_str(s: &str) -> Result<OutFormater, ()> {
//         match s {
//             "A" => Ok(OutFormater::Json),
//             "B" => Ok(OutFormater::Text),
//             _ => Err(()),
//         }
//     }
// }

// Json

struct JsonOutputFormater { }

impl OutputFormater for JsonOutputFormater{
    fn format_ok_line(&self, line: &OkLineOutput) -> String{
        serde_json::to_string(line)
            .unwrap_or_else(|e| panic!(e)) // should not happen
    }

    fn format_error_line(&self,  err: &ErrorLineOutput) -> String{
        serde_json::to_string(err)
            .unwrap_or_else(|e| panic!(e)) // should not happen
    }

    fn get_line_separator(&self) -> &'static str { "," }
    fn get_output_begin(&self) -> &'static str { "[" }
    fn get_output_end(&self) -> &'static str { "]" }
}

// Text
struct TextOutputFormater { }

impl OutputFormater for TextOutputFormater{
    fn format_ok_line(&self, line: &OkLineOutput) -> String{
        format!("line #{} : {} - {}", line.line_number, line.concat_ab, line.sum_cd)
    }

    fn format_error_line(&self, err: &ErrorLineOutput) -> String{
        format!("error as line {}: {}", err.line_number, err.error_message )
    }

    fn get_line_separator(&self) -> &'static str { "" }
    fn get_output_begin(&self) -> &'static str { "" }
    fn get_output_end(&self) -> &'static str { "" }
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

    // iterator: Box<csv::DeserializeRecordsIter<'r, &'r mut R, SourceLine>>,
    // iterator:     csv::DeserializeRecordsIter<'r, &'r mut R, SourceLine>,

struct CsvSourceIterator<'r, R: io::Read>{
    iterator: csv::DeserializeRecordsIter<'r, R, SourceLine>,
}

impl<'r, R: io::Read> CsvSourceIterator<'r, R> {
    // fn new(rdr : &'r R) -> CsvSourceIterator<'r, R> {
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


// impl<'r, R: io::Read> Iterator for CsvSourceIterator<'r, R>{
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
