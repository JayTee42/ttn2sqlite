use std::{convert::From, env, fmt};
use std::io::{self, BufRead, Error as IOError};
use base64::{self, decode_config_slice as base64_decode};
use rusqlite::{Connection, Error as SQLiteError, Statement, ToSql, NO_PARAMS};
use serde::{Deserialize, Deserializer, de::Error as _};
use serde_json::Error as JSONError;

// A universal error type for everything that can go wrong here:
enum Error
{
	Io(IOError),
	Json(JSONError),
	SQLite(SQLiteError),
}

impl fmt::Display for Error
{
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result
	{
		<Error as fmt::Debug>::fmt(self, f)
	}
}

impl fmt::Debug for Error
{
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result
	{
		match self
		{
			Error::Io(err) 		=> write!(f, "IO error ({:})", err),
			Error::Json(err) 	=> write!(f, "JSON error ({:})", err),
			Error::SQLite(err) 	=> write!(f, "SQLite error ({:})", err),
		}
	}
}

impl From<IOError> for Error
{
	fn from(err: IOError) -> Self
	{
		Error::Io(err)
	}
}

impl From<JSONError> for Error
{
	fn from(err: JSONError) -> Self
	{
		Error::Json(err)
	}
}

impl From<SQLiteError> for Error
{
	fn from(err: SQLiteError) -> Self
	{
		Error::SQLite(err)
	}
}

// The data format returned from TTN:
#[derive(Deserialize)]
struct UplinkMessage<'l>
{
	app_id: &'l str,
	dev_id: &'l str,
	hardware_serial: &'l str,
	port: u32,
	counter: u32,
	metadata: UplinkMetadata<'l>,

	// The payload is a blob of up to Payload::MAX_PAYLOAD_SIZE bytes.
	// It is stored as Base64 string (JSON field name is "payload_raw").
	// The function "deserialize_payload" (defined below) manages its deserialization.
	#[serde(rename = "payload_raw", deserialize_with = "deserialize_payload")]
	payload: Payload,
}

#[derive(Deserialize)]
struct UplinkMetadata<'l>
{
	time: &'l str,
	longitude: f64,
	latitude: f64,
	altitude: f64,
}

struct Payload
{
	bytes: [u8; Payload::MAX_PAYLOAD_SIZE],
	size: usize,
}

impl Payload
{
	// The maximum payload size in bytes, as defined by TTN:
	const MAX_PAYLOAD_SIZE: usize = 512;

	fn empty() -> Payload
	{
		Payload
		{
			bytes: [0; Payload::MAX_PAYLOAD_SIZE],
			size: 0,
		}
	}

	fn as_slice(&self) -> &[u8]
	{
		&self.bytes[0..self.size]
	}
}

// This function is responsible for deserializing the "raw_payload" JSON string into the "payload" field of our "UplinkMessage" struct.
fn deserialize_payload<'de, D>(deserializer: D) -> Result<Payload, D::Error>
	where D: Deserializer<'de>
{
	// Extract the JSOn value as string slice:
	let input = <&str as Deserialize>::deserialize(deserializer)?;

	// Decode the Base64 string into our array:
	let mut payload = Payload::empty();
	payload.size = base64_decode(input, base64::STANDARD, &mut payload.bytes).map_err(|err| D::Error::custom(err.to_string()))?;

	Ok(payload)
}

// This function deserializes a message from JSON into a struct.
// Then it tries to insert all the data into our DB.
fn process_line(line: &str, db_stmt: &mut Statement) -> Result<(), Error>
{
	// Try to deserialize the message:
	let msg: UplinkMessage = serde_json::from_str(&line)?;

	// Print some info about it:
	println!("Received uplink message (appID: \"{:}\", deviceID: \"{:}\", time: \"{:}\", payload: {:} bytes)", msg.app_id, msg.dev_id, msg.metadata.time, msg.payload.size);

	// Store it into our database:
	db_stmt.execute(&[&msg.app_id as &dyn ToSql, &msg.dev_id, &msg.hardware_serial, &msg.port, &msg.counter,
					&msg.metadata.time, &msg.metadata.longitude, &msg.metadata.latitude, &msg.metadata.altitude,
					&msg.payload.as_slice()])?;

	Ok(())
}

fn main() -> Result<(), Error>
{
	// Get the path to the DB as CLI argument.
	// If there is none, we use a default.
	let db_path = env::args().nth(1).unwrap_or(String::from("ttn_db.sqlite"));

	// Open the output database.
	// It may already exist.
	let db_connection = Connection::open(&db_path)?;

	// Create the data table if it is not yet there:
	db_connection.execute
	(
        "CREATE TABLE IF NOT EXISTS data
        (
        	app_id TEXT NOT NULL, dev_id TEXT NOT NULL, hardware_serial TEXT NOT NULL, port INTEGER NOT NULL, counter INTEGER NOT NULL,
        	time TEXT NOT NULL, lon REAL NOT NULL, lat REAL NOT NULL, alt REAL NOT NULL,
        	payload BLOB NOT NULL
        )",
        NO_PARAMS,
    )?;

    // Prepare a statement for insertion:
    let mut db_stmt = db_connection.prepare("INSERT INTO data (app_id, dev_id, hardware_serial, port, counter,
    											time, lon, lat, alt,
    											payload)
    											VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")?;

	// Read lines from stdin.
	// Each line represents a JSON-encoded uplink message.
	let stdin = io::stdin();

	for line in stdin.lock().lines()
	{
		// Try to read a new line from stdin and to parse it.
		// Print errors to the terminal (but don't kill the whole program).
		if let Err(err) = line.map_err(|err| err.into()).and_then(|l| process_line(&l, &mut db_stmt))
		{
			println!("Error while processing message:\n{:}", err);
		}
	}

	Ok(())
}
