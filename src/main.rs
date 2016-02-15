extern crate csv;
extern crate redis;
extern crate chrono;
#[macro_use] extern crate log;
#[macro_use] extern crate throw;
extern crate env_logger;

use std::sync::atomic::{AtomicBool, Ordering};
use redis::{Commands, PipelineCommands};
use std::path::PathBuf;
use std::env;
use log::{LogRecord, LogLevelFilter};
use env_logger::LogBuilder;
use std::io;
use chrono::*;
use std::io::prelude::*;
use std::fs::File;
use std::path::Path;
use std::fs::OpenOptions;

extern "C" {
  fn signal(sig: u32, cb: extern fn(u32));
}

extern fn interrupt(_:u32) {
	unsafe {
		stop_loop.as_ref().map(|z| z.store(true, Ordering::Relaxed));
	}
}

static mut stop_loop : Option<AtomicBool> = None;
const SERVER_UNIX_PATH: &'static str = "/tmp/redis.sock";
const TIMEOUT: usize = 1;

fn get_client_addr() -> redis::ConnectionAddr {
	redis::ConnectionAddr::Unix(PathBuf::from(SERVER_UNIX_PATH))
}

#[derive(Debug)]
enum MyError {Io(io::Error), 
	Redis(redis::RedisError), 
	Parser(chrono::format::ParseError)}

impl From<redis::RedisError> for MyError  {
    fn from(err: redis::RedisError) -> MyError {
        MyError::Redis(err)
    }
}

impl From<io::Error> for MyError {
    fn from(err: io::Error) -> MyError {
        MyError::Io(err)
    }	
}

impl From<chrono::format::ParseError> for MyError {
    fn from(err: chrono::format::ParseError) -> MyError {
        MyError::Parser(err)
    }	
}

impl<'a> From<&'a str> for MyError {
     fn from(err: &str) -> MyError {
         MyError::Io(io::Error::new(io::ErrorKind::Other, err))
     }	
}

fn open_file (file : &mut Option<File>, path : &Path) -> Result<(), throw::Error<MyError>> {
	info!("Opening File {:?}", path);
	let new_file = throw!(OpenOptions::new().write(true).append(true).create(true).open(path));
	*file = Some(new_file);
	Ok(())
}

fn close_file (file : &mut Option<File>) -> Result<(), throw::Error<MyError>> {
	file.as_ref().map(|mut f| { let _ = f.flush(); let _ = f.sync_all();});
	*file = None;
	Ok(())
}

fn format_utc(day_year : &Option<DateTime<UTC>>) -> Result<String, throw::Error<MyError>> {
	match *day_year {
	    Some(dt) => return Ok(dt.format("%Y%m%d").to_string()),
	    None => throw_new!("Illegal Argument None"),
	}
}

//fn format_filename()

fn change_day (day_year : &mut Option<DateTime<UTC>>, dt : DateTime<UTC>, file : &mut Option<File>) -> Result<(), throw::Error<MyError>> {
	*day_year = Some(dt);
	let filename = format!(r"fx{}.csv", up!(format_utc(day_year)));
	let path = Path::new(&filename);
	up!(close_file(file));
	up!(open_file(file, path));

	Ok(())
}

fn handle_item (item : &str, day_year : &mut Option<DateTime<UTC>>, file : &mut Option<File>) -> Result<(), throw::Error<MyError>> {
	let mut rdr = csv::Reader::from_string(item).has_headers(false).delimiter(b';');
	for row in rdr.records().map(|r| r.unwrap()) {
		if row.len() > 2 {
			//info!("{:?}", row[2]);
			let dt = throw!(UTC.datetime_from_str(row[2].as_ref(), "%Y.%m.%d %H:%M:%S"));
			// day_year.as_ref().map_or(*day_year = Some(dt), |expr| if expr.date() == dt.date() {*day_year = Some(dt);});
			match *day_year {
			    Some(expr) => if expr.date() != dt.date() {up!(change_day(day_year, dt, file));},
			    None => up!(change_day(day_year, dt, file)),
			}
			//info!("{:?}", dt.date());
			file.as_ref().map(|mut f| f.write(item.as_bytes()));
		}
		break;
	}
	Ok(())
}

fn handle(listname : &str, con: redis::Connection) -> Result<(), throw::Error<MyError>> {
 	let mut day_year : Option<DateTime<UTC>> = None;
 	let mut file : Option<File> = None;
   	loop {        
       	unsafe {
        	if stop_loop.as_ref().map_or(false, |z|z.load(Ordering::Relaxed)) {
            	break;
            }             
        }

        let pop : Option<(String, String)> = throw!(con.brpop(listname, TIMEOUT));
		match pop {
		    None => {},			
		    Some((_, item)) => {
		    			let _ = up!(handle_item(item.as_ref(), &mut day_year, &mut file));
			},
		} 
   	}
   	up!(close_file(&mut file));

	Ok(())   
}

fn main() {
	let listname = ::std::env::args().nth(1).unwrap();

    let format = |record: &LogRecord| {
        format!("{} - {}", record.level(), record.args())
    };

    let mut builder = LogBuilder::new();
    builder.format(format).filter(None, LogLevelFilter::Info);

    if env::var("RUST_LOG").is_ok() {
       builder.parse(&env::var("RUST_LOG").unwrap());
    }

    builder.init().unwrap();

    unsafe {
    	stop_loop = Some(AtomicBool::new(false));
      	signal(2, interrupt);    	
    }

    let client = redis::Client::open(redis::ConnectionInfo {
           addr: Box::new(get_client_addr()),
           db: 0,
           passwd: None,
    }).unwrap();
    let con = client.get_connection().unwrap();

    let result = handle(listname.as_ref(), con);
    match result {
        Err(e) => error!("{:?}", e),
        _ => info!("Normal termination !"),
    }

}

