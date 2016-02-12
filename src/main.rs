extern crate csv;
extern crate redis;
#[macro_use] extern crate log;
extern crate env_logger;
#[macro_use] extern crate throw;

use std::thread;
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool, Ordering};
use redis::{Commands, PipelineCommands};
use std::path::PathBuf;
use std::collections::HashMap;
use std::env;
use log::{LogRecord, LogLevelFilter};
use env_logger::LogBuilder;
use std::io::Error;
use redis::types::RedisError;

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

enum MyError {Io(Error), Redis(RedisError)}

impl From<RedisError> for MyError  {
    fn from(err: RedisError) -> MyError {
        MyError::Redis {}
    }
}

impl From<Error> for MyError {
    fn from(err: Error) -> MyError {
        MyError::Io {}
    }	
}

impl From<MyError> for Error {
    fn from(err: MyError) -> Error {
        Error {}
    }		
}

fn handle_item (item : &str) -> Result<(), throw::Error<Error>> {

	Ok(())
}

fn handle(listname : String, con: redis::Connection) -> Result<(), throw::Error<Error>> {

   loop {        
            unsafe {
                match stop_loop {
                    Some(ref z) => if z.load(Ordering::Relaxed) {break},
                    None => {},
                }                
            }
/*
        let result = con.brpop(listname, TIMEOUT);
        match result {
                Ok(Some((_, item))) => {
		    			info!("{:?}", item);
		    			let req = up!(handle_item(item.as_ref()));
					},
                Err(err) => throw!(err),					
				_ => {},
            }    
*/

        let pop : Option<(String, String)> = throw!(con.brpop(listname, TIMEOUT));
		match pop {
		    None => {},			
		    Some((_, item)) => {
		    			info!("{:?}", item);
		    			let req = up!(handle_item(item.as_ref()));
			},
		} 

   }

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


    let client = redis::Client::open(redis::ConnectionInfo {
           addr: Box::new(get_client_addr()),
           db: 0,
           passwd: None,
    }).unwrap();
    let con = client.get_connection().unwrap();
    let mut result = Ok(());
    unsafe {
    	stop_loop = Some(AtomicBool::new(false));
      	signal(2, interrupt);
    	result = handle(listname, con);
    }
    match result {
        Err(e) => error!("{:?}", e),
        _ => info!("Normal termination !"),
    }

}

