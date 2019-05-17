#![feature(await_macro, async_await)]

use std::{fmt, fmt::Display, fmt::Formatter, io};

#[macro_use]
extern crate serde_derive;

use actix_web::{
    http, middleware, server, Query,
    /*HttpRequest, HttpResponse, Error, Body, Path,*/ Responder, Result,
};

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
        .subcommand(
            clap::SubCommand::with_name("process")
                .about("Run transformation from stdin or a file.")
                .arg(clap::Arg::with_name("file").short("f").takes_value(true))
                .arg(
                    clap::Arg::with_name("out")
                        .short("o")
                        .takes_value(true)
                        .default_value("json")
                        .possible_values(&["json", "text"]),
                ),
        )
        .subcommand(
            clap::SubCommand::with_name("webserver")
                .about("Run transformation through a rest API.")
                .arg(
                    clap::Arg::with_name("port")
                        .short("p")
                        .takes_value(true)
                        .default_value("4242"),
                ),
        )
        .get_matches();

    match arguments.subcommand() {
        ("process", Some(command_matches)) => {
            let formater = get_formater(command_matches.value_of("out").unwrap()).unwrap();
            let input = io::stdin();
            let mut output = io::stdout();
            process(formater, input, &mut output);
        }
        ("webserver", Some(webserver_matches)) => {
            let port = webserver_matches
                .value_of("port")
                .unwrap()
                .parse::<i32>()
                .unwrap();

            let sys = actix::System::new("example"); // <- create Actix system

            let address = format!("0.0.0.0:{}", port);
            server::new(|| {
                actix_web::App::new()
                    .middleware(middleware::Logger::default())
                    .resource(
                        "/filter",
                        |r| r.method(http::Method::GET).with(filter), // .with(compat(filter))
                    )
            })
            .bind(&address)
            .unwrap_or_else(|_| panic!("Can not bind to {}.", &address))
            .start();


            sys.run(); //.expect("Something went wrong"); <- for actix 0.8
        }
        (_, None) => {
            println!("Use a subcommand : process or webservice. see help for more informations.")
        }
        _ => unreachable!(),
    }
}


// TODO : https://github.com/actix/examples/blob/master/multipart/src/main.rs

#[derive(Debug)]
enum BzError {
    FormatterNotExisting(String),
}

// could be better : https://github.com/actix/actix-website/blob/master/content/docs/errors.md : See "Recommended practices in error handling"
impl Display for BzError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            BzError::FormatterNotExisting(s) => write!(f, "Output formatter '{}' not supported", s),
        }
    }
}

#[derive(Deserialize, Debug)]
struct Info {
    #[serde(rename = "csvUri")]
    csv_uri: String,
    #[serde(default = "default_formater")]
    format: String,
}

fn default_formater() -> String {
    "json".to_string()
}


// async fn filter(info: Query<Info>) -> Result<String, Error> {
//     // Wait 2s
//     await!(Delay::new(Instant::now() + Duration::from_secs(2)))?;

//     // Proceed with normal response
//     Ok(format!("Hello {}! id:{}", info.csv_uri, info.format))
// }


// https://docs.rs/actix-web/0.4.5/src/actix_web/fs.rs.html#166-231
// http://localhost:8000/12654/alzifg/index.html

/*
https://actix.rs/docs/response/
https://www.google.com/search?q=rust+future+stream+to+iterator
https://docs.rs/futures/0.1/futures/stream/fn.iter.html
*/

/*
https://users.rust-lang.org/t/adapter-for-transforming-io-write-into-stream/27324
https://stackoverflow.com/questions/55708392/how-to-send-data-through-a-futures-stream-by-writing-through-the-iowrite-trait/55764246#55764246


// TO READ !!!
//https://cetra3.github.io/blog/face-detection-with-actix-web/

*/

// https://stackoverflow.com/questions/56023741/how-to-stream-iowrite-into-an-actix-web-response

// use futures::{
//     sink::{Sink, Wait},
//     sync::mpsc,
// }; // 0.1.26
// use std::{thread};

// fn generate(_output: &mut io::Write) {
//     // ...
// }

// struct MyWrite<T>(Wait<mpsc::Sender<T>>);

// impl<T> io::Write for MyWrite<T>
// where
//     T: for<'a> From<&'a [u8]> + Send + Sync + 'static,
// {
//     fn write(&mut self, d: &[u8]) -> io::Result<usize> {
//         let len = d.len();
//         self.0
//             .send(d.into())
//             .map(|()| len)
//             .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
//     }

//     fn flush(&mut self) -> io::Result<()> {
//         self.0
//             .flush()
//             .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
//     }
// }

// fn foo() -> impl futures::Stream<Item = Vec<u8>, Error = ()> {
//     let (tx, rx) = mpsc::channel(5);

//     let mut w = MyWrite(tx.wait());

//     thread::spawn(move || generate(&mut w));

//     rx
// }

// struct MyError{}
// impl From<reqwest::Error> for MyError{
//     fn from(err: reqwest::Error) -> MyError {
//         MyError{}
//     }
// }
// impl From<std::str::Utf8Error> for MyError{
//     fn from(err: std::str::Utf8Error) -> MyError {
//         MyError{}
//     }
// }


fn filter(info: Query<Info>) -> Result<impl Responder> {
    let csv = reqwest::get(&info.csv_uri).map_err(actix_web::error::ErrorBadRequest)?;
    let formater = get_formater(&info.format).map_err(actix_web::error::ErrorBadRequest)?;
    let mut buffer: Vec<u8> = vec![];

    process(formater, csv, &mut buffer);

    let output = std::str::from_utf8(&buffer[..])?;
    Ok(output.to_string())
}

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

fn process<R: io::Read, W: io::Write>(formater: Box<OutputFormater>, reader: R, writer: &mut W) {
    let cvs_iter = CsvSourceIterator::new(reader);

    let line_separator = formater.get_line_separator();

    writeln!(writer, "{}", formater.get_output_begin()).expect("write error");

    for (count, result) in cvs_iter.enumerate() {
        match result {
            Ok(record) => {
                let record: SourceLine = record;
                if let (Some(c), Some(d)) = (record.column_c, record.column_d) {
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
                        write_line(writer, output, count, line_separator);
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
                write_line(writer, output, count, line_separator);
            }
        }
    }

    writeln!(writer, "{}", formater.get_output_end()).expect("write error");;
}

fn write_line<W: io::Write>(
    writer: &mut W,
    output: String,
    line_count: usize,
    line_separator: &str,
) {
    let mut output_line = if line_count > 1 {
        String::from(line_separator)
    } else {
        String::default()
    };
    output_line.push_str(&output);
    writeln!(writer, "{}", output_line).expect("write error");
}

// ***************************************************
//                      TARGET
// ***************************************************

fn get_formater(output_type_name: &str) -> Result<Box<OutputFormater>, BzError> {
    match output_type_name {
        "json" => Ok(Box::new(JsonOutputFormater {})),
        "text" => Ok(Box::new(TextOutputFormater {})),
        _ => Err(BzError::FormatterNotExisting(output_type_name.to_owned())),
    }
}

#[derive(Debug, Serialize)]
struct OkLineOutput {
    #[serde(rename = "lineNumber")]
    line_number: usize,
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
    line_number: usize,
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

struct JsonOutputFormater {}

impl OutputFormater for JsonOutputFormater {
    fn format_ok_line(&self, line: &OkLineOutput) -> String {
        serde_json::to_string(line).unwrap_or_else(|e| panic!(e)) // should not happen
    }

    fn format_error_line(&self, err: &ErrorLineOutput) -> String {
        serde_json::to_string(err).unwrap_or_else(|e| panic!(e)) // should not happen
    }

    fn get_line_separator(&self) -> &'static str {
        ","
    }
    fn get_output_begin(&self) -> &'static str {
        "["
    }
    fn get_output_end(&self) -> &'static str {
        "]"
    }
}

// Text
struct TextOutputFormater {}

impl OutputFormater for TextOutputFormater {
    fn format_ok_line(&self, line: &OkLineOutput) -> String {
        format!(
            "line #{} : {} - {}",
            line.line_number, line.concat_ab, line.sum_cd
        )
    }

    fn format_error_line(&self, err: &ErrorLineOutput) -> String {
        format!("error as line {}: {}", err.line_number, err.error_message)
    }

    fn get_line_separator(&self) -> &'static str {
        ""
    }
    fn get_output_begin(&self) -> &'static str {
        ""
    }
    fn get_output_end(&self) -> &'static str {
        ""
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
    column_c: Option<i64>,
    #[serde(rename = "columnD")]
    column_d: Option<i64>,
    #[serde(rename = "otherColumn")]
    other_column: Option<String>,
}

struct CsvSourceIterator<R: io::Read> {
    iterator: csv::DeserializeRecordsIntoIter<R, SourceLine>,
}

impl<R: io::Read> CsvSourceIterator<R> {
    fn new(rdr: R) -> CsvSourceIterator<R> {
        let reader = csv::ReaderBuilder::new()
            .delimiter(b';')
            .flexible(true)
            .from_reader(rdr);

        let csv = reader.into_deserialize();

        CsvSourceIterator { iterator: csv }
    }
}

impl<R: io::Read> Iterator for CsvSourceIterator<R> {
    type Item = Result<SourceLine, csv::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next()
    }
}

