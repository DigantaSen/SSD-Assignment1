// ============================
// Q2(b.1) — Top 5 hottest days nationwide
// ============================

print("\n[Q2(b.1)] Top 5 hottest days nationwide:\n");

db.temps.aggregate([
  { $set: { dateObj: { $dateFromString: { dateString: "$date", onError: null, onNull: null } } } },
  { $set: { avg_temp: { $toDouble: "$temp.avg_c" } } },
  { $group: { _id: "$dateObj", nationwide_avg_temp: { $avg: "$avg_temp" } } },
  { $sort: { nationwide_avg_temp: -1 } },
  { $limit: 5 },
  { $project: {
      _id: 0,
      date: { $dateToString: { format: "%Y-%m-%d", date: "$_id" } },
      nationwide_avg_temp: { $round: ["$nationwide_avg_temp", 2] }
  }}
]).forEach(doc => print(EJSON.stringify(doc)));



// ============================
// Q2(b.2) — Top 5 coldest days nationwide
// ============================

print("\n[Q2(b.2)] Top 5 coldest days nationwide:\n");

db.temps.aggregate([
  { $set: { dateObj: { $dateFromString: { dateString: "$date", onError: null, onNull: null } } } },
  { $set: { avg_temp: { $toDouble: "$temp.avg_c" } } },
  { $group: { 
      _id: "$dateObj", 
      nationwide_avg_temp: { $avg: "$avg_temp" } 
  }},
  { $sort: { nationwide_avg_temp: 1 } },
  { $limit: 5 },
  { $project: {
      _id: 0,
      date: { $dateToString: { format: "%Y-%m-%d", date: "$_id" } },
      nationwide_avg_temp: { $round: ["$nationwide_avg_temp", 2] }
  }},
  { $sort: { date: 1 } }
]).forEach(doc => print(EJSON.stringify(doc)));



// ============================
// Q2(b.3) — Check if it rained
// ============================

var cityToCheck = "Navi Mumbai";
var dateToCheck = "2025-06-17";

print(`\n[Q2(b.3)] Was it raining in ${cityToCheck} on ${dateToCheck}?\n`);

var rain = db.weather.aggregate([
  { $match: { city: cityToCheck, date: dateToCheck } },
  { 
    $group: { 
      _id: null, 
      condition: { $first: "$condition" }, 
      totalPrecip: { $sum: { $ifNull: ["$precip_mm", 0] } } 
    } 
  }
]).toArray()[0];

if (rain) {
  var output = {
    city: cityToCheck,
    date: dateToCheck,
    rained: rain.totalPrecip > 0,
    condition: rain.condition,
    precipitation_mm: rain.totalPrecip
  };
  print(EJSON.stringify(output));
} else {
  print(EJSON.stringify({
    city: cityToCheck,
    date: dateToCheck,
    rained: false,
    condition: null,
    precipitation_mm: 0
  }));
}


// ============================
// Q2(b.4) — 7-day moving average
// ============================

var cityToCheck = "Delhi";
var startDate = "2025-05-24";
var endDate = "2025-06-14";

print(`\n[Q2(b.4)] 7-day moving average for ${cityToCheck} (${startDate} to ${endDate}):\n`);

db.temps.aggregate([
  { $match: { city: cityToCheck } },
  { $set: { dateObj: { $dateFromString: { dateString: "$date", onError: null, onNull: null } } } },
  { $set: { avg_temp: { $toDouble: "$temp.avg_c" } } },
  { $sort: { dateObj: 1 } },
  { $setWindowFields: {
      partitionBy: "$city",
      sortBy: { dateObj: 1 },
      output: {
        seven_day_avg_temp: {
          $avg: "$avg_temp",
          window: { documents: [-6, 0] }
        }
      }
  }},
  { $project: {
      _id: 0,
      date: { $dateToString: { format: "%Y-%m-%d", date: "$dateObj" } },
      avg_c: "$avg_temp",
      seven_day_avg_temp: { $round: ["$seven_day_avg_temp", 6] }
  }},
  { $match: { date: { $gte: startDate, $lte: endDate } } },
  { $sort: { date: 1 } }
]).forEach(doc => print(EJSON.stringify(doc)));

