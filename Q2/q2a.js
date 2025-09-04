// Q2(a.1) — Daily average temperature per city by month
print("\n[Q2(a.1)] Daily average temperature per city by month:\n");
db.temps.aggregate([
  { $set: { dateObj: { $dateFromString: { dateString: "$date" } } } },
  {
    $project: {
      city: 1,
      month: { $month: "$dateObj" },
      day: "$dateObj",
      val: { $convert: { input: "$temp.avg_c", to: "double", onError: null, onNull: null } }
    }
  },
  { $group: { _id: { city: "$city", month: "$month", day: "$day" }, daily_avg_temp: { $avg: "$val" } } },
  { $project: { _id: 1, daily_avg_temp: { $round: ["$daily_avg_temp", 2] } } },
  { $sort: { "_id.city": 1, "_id.month": 1, "_id.day": 1 } }
]).forEach(doc => print(EJSON.stringify(doc)));


// Q2(a.2) — Monthly average temperature per city
print("\n[Q2(a.2)] Monthly average temperature per city:\n");
db.temps.aggregate([
  { $set: { dateObj: { $dateFromString: { dateString: "$date" } } } },
  {
    $project: {
      city: 1,
      month: { $month: "$dateObj" },
      val: { $convert: { input: "$temp.avg_c", to: "double", onError: null, onNull: null } }
    }
  },
  { $group: { _id: { city: "$city", month: "$month" }, monthly_avg_temp: { $avg: "$val" } } },
  { $project: { _id: 1, monthly_avg_temp: { $round: ["$monthly_avg_temp", 2] } } },
  { $sort: { "_id.city": 1, "_id.month": 1 } }
]).forEach(doc => print(EJSON.stringify(doc)));


// Q2(a.3) — Hottest and coldest cities overall
print("\n[Q2(a.3)] Hottest and coldest cities overall:\n");
var hottest = db.temps.aggregate([
  { $project: { city: 1, val: { $convert: { input: "$temp.avg_c", to: "double", onError: null, onNull: null } } } },
  { $group: { _id: "$city", avgTemp: { $avg: "$val" } } },
  { $sort: { avgTemp: -1 } },
  { $limit: 1 }
]).toArray()[0];
if (hottest) print(EJSON.stringify({ hottest_city: hottest._id, avgTemp: Math.round(hottest.avgTemp * 100) / 100 }));

var coldest = db.temps.aggregate([
  { $project: { city: 1, val: { $convert: { input: "$temp.avg_c", to: "double", onError: null, onNull: null } } } },
  { $group: { _id: "$city", avgTemp: { $avg: "$val" } } },
  { $sort: { avgTemp: 1 } },
  { $limit: 1 }
]).toArray()[0];
if (coldest) print(EJSON.stringify({ coldest_city: coldest._id, avgTemp: Math.round(coldest.avgTemp * 100) / 100 }));
