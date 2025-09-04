// Q2(b.1) — Top 5 hottest days nationwide
print("\n[Q2(b.1)] Top 5 hottest days nationwide:\n");
db.temps.aggregate([
  { $set: { dateObj: { $dateFromString: { dateString: "$date" } } } },
  { $project: { dateObj: 1, val: { $convert: { input: "$temp.avg_c", to: "double", onError: null, onNull: null } } } },
  { $group: { _id: "$dateObj", nationwide_avg_temp: { $avg: "$val" } } },
  { $sort: { nationwide_avg_temp: -1 } },
  { $limit: 5 }
]).forEach(doc => print(EJSON.stringify(doc)));


// Q2(b.2) — Top 5 coldest days nationwide
print("\n[Q2(b.2)] Top 5 coldest days nationwide:\n");
db.temps.aggregate([
  { $set: { dateObj: { $dateFromString: { dateString: "$date" } } } },
  { $project: { dateObj: 1, val: { $convert: { input: "$temp.avg_c", to: "double", onError: null, onNull: null } } } },
  { $group: { _id: "$dateObj", nationwide_avg_temp: { $avg: "$val" } } },
  { $sort: { nationwide_avg_temp: 1 } },
  { $limit: 5 }
]).forEach(doc => print(EJSON.stringify(doc)));


// Q2(b.3) — Was it raining in Mumbai on 2025-06-15?
print("\n[Q2(b.3)] Was it raining in Mumbai on 2025-06-15?\n");
var rain = db.temps.aggregate([
  { $match: { city: "Mumbai", date: "2025-06-15" } },
  { $group: { _id: null, condition: { $first: "$condition" }, totalPrecip: { $sum: { $ifNull: ["$precip_mm", 0] } } } }
]).toArray()[0];
if (rain) {
  var rained = rain.totalPrecip > 0;
  print(EJSON.stringify({ city: "Mumbai", date: "2025-06-15", rained: rained, condition: rain.condition, precipitation_mm: rain.totalPrecip }));
} else {
  print(EJSON.stringify({ city: "Mumbai", date: "2025-06-15", rained: false, condition: null, precipitation_mm: 0 }));
}


// Q2(b.4) — 7-day moving average for Delhi (last 10 days of June)
print("\n[Q2(b.4)] 7-day moving average for Delhi (last 10 days of June):\n");
db.temps.aggregate([
  { $match: { city: "Delhi" } },
  { $set: { dateObj: { $dateFromString: { dateString: "$date" } } } },
  { $sort: { dateObj: 1 } },
  { $setWindowFields: {
      partitionBy: "$city",
      sortBy: { dateObj: 1 },
      output: {
        seven_day_avg_temp: { $avg: { $convert: { input: "$temp.avg_c", to: "double", onError: null, onNull: null } }, window: { documents: [-6, 0] } }
      }
  }},
  { $project: { _id: 0, date: "$dateObj", avg_c: { $convert: { input: "$temp.avg_c", to: "double", onError: null, onNull: null } }, seven_day_avg_temp: { $round: ["$seven_day_avg_temp", 6] } } },
  { $match: { date: { $gte: ISODate("2025-06-21T00:00:00Z"), $lte: ISODate("2025-06-30T23:59:59Z") } } },
  { $sort: { date: 1 } }
]).forEach(doc => print(EJSON.stringify(doc)));
