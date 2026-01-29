use std::error::Error;
use std::io::Cursor;
use std::time::Instant;

use curl::easy::Easy;
use polars::prelude::*;
use protobuf::Message;
use rayon::prelude::*;

mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
}

use protos::gtfs_realtime::*;

fn fetch_ztm_data(file: &str) -> Result<Vec<u8>, curl::Error> {
    let url = format!("https://www.ztm.poznan.pl/pl/dla-deweloperow/getGtfsRtFile?file={file}");
    let mut data = Vec::new();
    let mut easy = Easy::new();
    easy.url(&url)?;

    {
        let mut transfer = easy.transfer();
        transfer.write_function(|newdata| {
            data.extend_from_slice(newdata);
            Ok(newdata.len())
        })?;
        transfer.perform()?;
    }
    Ok(data)
}

fn main() -> Result<(), Box<dyn Error>> {
    let files = [
        "feeds.pb",
        "trip_updates.pb",
        "vehicle_positions.pb",
        "vehicle_dictionary.csv",
    ];
    let time1 = Instant::now();
    let d: Vec<Vec<u8>> = files
        .par_iter()
        .map(|&file| fetch_ztm_data(file))
        .collect::<Result<Vec<_>, _>>()?;
    let time1e = time1.elapsed();

    let (feeds, trip_updates, vehicle_positions, vehicle_dictionary) = (&d[0], &d[1], &d[2], &d[3]);
    let time2 = Instant::now();

    let vehicle_dictionary_df = CsvReader::new(Cursor::new(vehicle_dictionary))
        .with_options(CsvReadOptions::default().with_infer_schema_length(None))
        .finish()?;

    let feeds_df = match FeedMessage::parse_from_bytes(feeds) {
        Ok(feed) => {
            let mut entity_ids = Vec::new();
            let mut has_trip_update = Vec::new();
            let mut has_vehicle_position = Vec::new();
            let mut has_alert = Vec::new();

            for entity in &feed.entity {
                entity_ids.push(entity.id().to_string());
                has_trip_update.push(entity.trip_update.is_some());
                has_vehicle_position.push(entity.vehicle.is_some());
                has_alert.push(entity.alert.is_some());
            }

            df!(
                "entity_id" => entity_ids,
                "has_trip_update" => has_trip_update,
                "has_vehicle_position" => has_vehicle_position,
                "has_alert" => has_alert,
            )?
        }
        Err(e) => {
            eprintln!("Failed to parse feeds: {}", e);
            df!("error" => ["Failed to parse feeds"])?
        }
    };

    let trip_updates_df = match FeedMessage::parse_from_bytes(trip_updates) {
        Ok(feed) => {
            let mut entity_ids = Vec::new();
            let mut trip_ids = Vec::new();
            let mut route_ids = Vec::new();
            let mut start_times = Vec::new();
            let mut start_dates = Vec::new();
            let mut num_stop_updates = Vec::new();

            for entity in &feed.entity {
                if entity.trip_update.is_some() {
                    let trip_update = entity.trip_update.as_ref().unwrap();
                    entity_ids.push(entity.id().to_string());

                    if trip_update.trip.is_some() {
                        let trip = trip_update.trip.as_ref().unwrap();
                        trip_ids.push(trip.trip_id.clone().unwrap_or_default());
                        route_ids.push(trip.route_id.clone().unwrap_or_default());
                        start_times.push(trip.start_time.clone().unwrap_or_default());
                        start_dates.push(trip.start_date.clone().unwrap_or_default());
                    } else {
                        trip_ids.push(String::new());
                        route_ids.push(String::new());
                        start_times.push(String::new());
                        start_dates.push(String::new());
                    }

                    num_stop_updates.push(trip_update.stop_time_update.len() as i64);
                }
            }

            df!(
                "entity_id" => entity_ids,
                "trip_id" => trip_ids,
                "route_id" => route_ids,
                "start_time" => start_times,
                "start_date" => start_dates,
                "num_stop_updates" => num_stop_updates,
            )?
        }
        Err(e) => {
            eprintln!("Failed to parse trip updates: {}", e);
            df!("error" => ["Failed to parse trip updates"])?
        }
    };

    let vehicle_positions_df = match FeedMessage::parse_from_bytes(vehicle_positions) {
        Ok(feed) => {
            let mut entity_ids = Vec::new();
            let mut vehicle_ids = Vec::new();
            let mut vehicle_labels = Vec::new();
            let mut latitudes = Vec::new();
            let mut longitudes = Vec::new();
            let mut bearings = Vec::new();
            let mut speeds = Vec::new();
            let mut trip_ids = Vec::new();
            let mut route_ids = Vec::new();

            for entity in &feed.entity {
                if entity.vehicle.is_some() {
                    let vehicle = entity.vehicle.as_ref().unwrap();
                    entity_ids.push(entity.id().to_string());

                    if vehicle.vehicle.is_some() {
                        let veh_desc = vehicle.vehicle.as_ref().unwrap();
                        vehicle_ids.push(veh_desc.id.clone().unwrap_or_default());
                        vehicle_labels.push(veh_desc.label.clone().unwrap_or_default());
                    } else {
                        vehicle_ids.push(String::new());
                        vehicle_labels.push(String::new());
                    }

                    if vehicle.position.is_some() {
                        let pos = vehicle.position.as_ref().unwrap();
                        latitudes.push(pos.latitude());
                        longitudes.push(pos.longitude());
                        bearings.push(pos.bearing.unwrap_or(0.0));
                        speeds.push(pos.speed.unwrap_or(0.0));
                    } else {
                        latitudes.push(0.0);
                        longitudes.push(0.0);
                        bearings.push(0.0);
                        speeds.push(0.0);
                    }

                    if vehicle.trip.is_some() {
                        let trip = vehicle.trip.as_ref().unwrap();
                        trip_ids.push(trip.trip_id.clone().unwrap_or_default());
                        route_ids.push(trip.route_id.clone().unwrap_or_default());
                    } else {
                        trip_ids.push(String::new());
                        route_ids.push(String::new());
                    }
                }
            }

            df!(
                "entity_id" => entity_ids,
                "vehicle_id" => vehicle_ids,
                "vehicle_label" => vehicle_labels,
                "latitude" => latitudes,
                "longitude" => longitudes,
                "bearing" => bearings,
                "speed" => speeds,
                "trip_id" => trip_ids,
                "route_id" => route_ids,
            )?
        }
        Err(e) => {
            eprintln!("Failed to parse vehicle positions: {}", e);
            df!("error" => ["Failed to parse vehicle positions"])?
        }
    };

    let time2e = time2.elapsed();

    println!("\n=== Vehicle Dictionary ===");
    println!("{}", vehicle_dictionary_df);
    println!(
        "{:?}",
        vehicle_dictionary_df
            .get_columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect::<Vec<String>>()
    );

    println!("\n=== Feeds ===");
    println!("{}", feeds_df);
    println!(
        "{:?}",
        feeds_df
            .get_columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect::<Vec<String>>()
    );

    println!("\n=== Trip Updates ===");
    println!("{}", trip_updates_df);
    println!(
        "{:?}",
        trip_updates_df
            .get_columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect::<Vec<String>>()
    );

    println!("\n=== Vehicle Positions ===");
    println!("{}", vehicle_positions_df);
    println!(
        "{:?}",
        vehicle_positions_df
            .get_columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect::<Vec<String>>()
    );
    println!("{:?}", vehicle_positions_df.column("speed")?.mean_reduce());

    println!("TIME SPENT DOWNLOADING DATA = {:?}", time1e);
    println!("TIME SPENT CONSTRUCTING DATA = {:?}", time2e);

    Ok(())
}
