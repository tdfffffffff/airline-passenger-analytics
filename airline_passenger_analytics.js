// Qn 1 
//Step 1: Exploring data
db.customer_support.aggregate([
  {
    $group: {
      _id: "$category",
      count: { $sum: 1 }
    }
  },
  {
    $facet: {
      categoryCounts: [
        {
          $project: {
            category: "$_id", 
            count: 1,
            _id: 0           
          }
        }
      ],
      uniqueCategoryCount: [
        {
          $count: "unique_category" // There are a total of 36 unique categories. However, upon examination, not all of them are relevant for our analysis and thus data cleaning is needed.
        }
      ]
    }
  }
])

// It seems like only the capitalised entries in the `category` column clearly represent defined categories of customer support queries, while the remaining 28 rows contain random texts and results that do not fit as customer support queries. 
// Hence, since a substantial amount of irrelevant data (28 rows) can undermine the integrity of our dataset, we will be cleaning the data by removing these rows for a more accurate analysis.

//Step 2: Clean data by removing irrelevant categories
db.customer_support.deleteMany({
  category: { $nin: ["SHIPPING", "REFUND", "PAYMENT", "ORDER", "INVOICE", "FEEDBACK", "CONTACT", "CANCEL"] }
})

//Step 3: Display the remaining categories
db.customer_support.aggregate([
  {
    $match: {
      category: { $in: ["SHIPPING", "REFUND", "PAYMENT", "ORDER", "INVOICE", "FEEDBACK", "CONTACT", "CANCEL"] }
    }
  },
  {
    $group: {
      _id: "$category",
      NumberOfEntries: { $sum: 1 }
    }
  },
  {
    $sort: { _id: -1 } // Sort categories in descending order
  },
  {
    $project: {
      Category: "$_id",
      NumberOfEntries: 1,
      _id: 0
    }
  }
]) // Thus, after cleaning the data, there are 8 categories in customer_support, namely 'SHIPPING', 'REFUND', 'PAYMENT', 'ORDER', 'INVOICE', 'FEEDBACK', 'CONTACT', 'CANCEL'.

// Qn 2 
db.customer_support.aggregate([
  {
    $project: {
      category: 1,
      hasBothFlags: {
        $and: [
          { $regexMatch: { input: "$flags", regex: /Q/ } },
          { $regexMatch: { input: "$flags", regex: /W/ } }
        ]
      }
    }
  },
  {
    $group: {
      _id: "$category",
      count: { $sum: { $cond: ["$hasBothFlags", 1, 0] } }
    }
  },
  {
    $sort: { _id: 1 }
  }
]);

// Qn 3
db.flight_delay.aggregate([
  {
    $match: { Cancelled: 1 }
  },
  {
    $group: {
      _id: "$Airline",
      numOfFlights: { $sum: 1 }
    }
  },
  {
    $addFields: { Status: "Cancelled" }
  },
  {
    $unionWith: {
      coll: "flight_delay",
      pipeline: [
        {
          $match: { ArrDelay: { $gt: 0 } }
        },
        {
          $group: {
            _id: "$Airline",
            numOfFlights: { $sum: 1 }
          }
        },
        {
          $addFields: { Status: "Delayed" }
        }
      ]
    }
  },
  {
    $sort: { _id: 1, Status: 1 }
  }
]);

// Qn 4
db.flight_delay.aggregate([
  // Step 1: Parse Date field and filter delays within range
  {
    $addFields: {
      parsedDate: { $dateFromString: { dateString: "$Date", format: "%d-%m-%Y" } }
    }
  },
  {
    $match: {
      ArrDelay: { $gt: 0 },
      parsedDate: {
        $gte: ISODate("2019-01-01T00:00:00Z"),
        $lte: ISODate("2019-12-06T23:59:59Z")
      }
    }
  },
  // Step 2: Add MonthName and Route fields
  {
    $addFields: {
      MonthName: { $dateToString: { format: "%B", date: "$parsedDate" } },
      Route: { $concat: ["$Origin", " - ", "$Dest"] }
    }
  },
  // Step 3: Group by MonthName and Route, and count delay instances
  {
    $group: {
      _id: { MonthName: "$MonthName", Route: "$Route" },
      DelayInstances: { $sum: 1 }
    }
  },
  // Step 4: Add MonthName and Route back to fields, sort by delays
  {
    $addFields: {
      MonthName: "$_id.MonthName",
      Route: "$_id.Route"
    }
  },
  {
    $sort: { "MonthName": 1, "DelayInstances": -1 }
  },
  // Step 5: Group by MonthName to select the top Route (simulate RANK = 1)
  {
    $group: {
      _id: "$MonthName",
      topRoute: { $first: "$Route" },
      DelayInstances: { $first: "$DelayInstances" }
    }
  },
  // Step 6: Add custom order for months to sort in calendar order
  {
    $addFields: {
      monthOrder: {
        $switch: {
          branches: [
            { case: { $eq: ["$_id", "January"] }, then: 1 },
            { case: { $eq: ["$_id", "February"] }, then: 2 },
            { case: { $eq: ["$_id", "March"] }, then: 3 },
            { case: { $eq: ["$_id", "April"] }, then: 4 },
            { case: { $eq: ["$_id", "May"] }, then: 5 },
            { case: { $eq: ["$_id", "June"] }, then: 6 },
            { case: { $eq: ["$_id", "July"] }, then: 7 },
            { case: { $eq: ["$_id", "August"] }, then: 8 },
            { case: { $eq: ["$_id", "September"] }, then: 9 },
            { case: { $eq: ["$_id", "October"] }, then: 10 },
            { case: { $eq: ["$_id", "November"] }, then: 11 },
            { case: { $eq: ["$_id", "December"] }, then: 12 }
          ],
          default: 13
        }
      }
    }
  },
  { $sort: { monthOrder: 1 } },
  // Step 7: Output final results
  {
    $project: {
      MonthName: "$_id",
      Route: "$topRoute",
      DelayInstances: 1,
      _id: 0
    }
  }
]);

// Qn 5
use grp_proj

db.sia_stock.find()

db.sia_stock.aggregate([
    {
        $addFields: {
            stockDateConverted: {
                $dateFromString: {
                    dateString: "$StockDate",
                    format: "%m/%d/%Y"
                }
            },
            year: { $year: { $dateFromString: { dateString: "$StockDate", format: "%m/%d/%Y" } } },
            month: { $month: { $dateFromString: { dateString: "$StockDate", format: "%m/%d/%Y" } } }
        }
    },
    {
        $addFields: {
            monthName: {
                $arrayElemAt: [
                    [
                        "January", "February", "March", "April", "May", "June", 
                        "July", "August", "September", "October", "November", "December"
                    ], 
                    { $subtract: ["$month", 1] }
                ]
            }
        }
    },
    {
        $group: {
            _id: { year: "$year", monthNumber: "$month", monthName: "$monthName" }
        }
    },
    {
        $project: {
            _id: 0,
            year: "$_id.year",
            monthNumber: "$_id.monthNumber",
            monthName: "$_id.monthName"
        }
    },
    {
        $sort: { year: 1, monthNumber: 1 }
    }
]);


db.sia_stock.aggregate([
    {
        $addFields: {
            stockDateConverted: {
                $dateFromString: {
                    dateString: "$StockDate",
                    format: "%m/%d/%Y"
                }
            }
        }
    },
    {
        $group: {
            _id: null,
            earliestDate: { $min: "$stockDateConverted" },
            latestDate: { $max: "$stockDateConverted" }
        }
    },
    {
        $project: {
            _id: 0,
            earliest_date: "$earliestDate",
            latest_date: "$latestDate"
        }
    }
]);


db.sia_stock.aggregate([
    {
        $addFields: {
            stockDateConverted: {
                $dateFromString: {
                    dateString: "$StockDate",
                    format: "%m/%d/%Y"
                }
            },
            year: { 
                $year: { 
                    $dateFromString: { 
                        dateString: "$StockDate", 
                        format: "%m/%d/%Y" 
                    } 
                }
            }
        }
    },
    {
        $match: {
            year: 2023
        }
    }
]);


db.sia_stock.aggregate([
    {
        $addFields: {
            stockDateConverted: {
                $dateFromString: {
                    dateString: "$StockDate",
                    format: "%m/%d/%Y"
                }
            },
            year: { 
                $year: { 
                    $dateFromString: { 
                        dateString: "$StockDate", 
                        format: "%m/%d/%Y" 
                    } 
                }
            },
            month: { 
                $month: { 
                    $dateFromString: { 
                        dateString: "$StockDate", 
                        format: "%m/%d/%Y" 
                    } 
                }
            }
        }
    },
    {
        $match: {
            year: 2023
        }
    },
    {
        $group: {
            _id: { year: "$year", month: "$month" },
            avg_high_price: { $avg: "$High" },
            avg_low_price: { $avg: "$Low" },
            avg_price: { 
                $avg: { $divide: [{ $add: ["$High", "$Low"] }, 2] }
            }
        }
    },
    {
        $project: {
            _id: 0,
            year: "$_id.year",
            month: "$_id.month",
            avg_high_price: { $round: ["$avg_high_price", 2] },
            avg_low_price: { $round: ["$avg_low_price", 2] },
            avg_price: { $round: ["$avg_price", 2] }
        }
    },
    {
        $sort: {
            month: 1
        }
    }
]);


db.sia_stock.aggregate([
    {
        $addFields: {
            stockDateConverted: {
                $dateFromString: {
                    dateString: "$StockDate",
                    format: "%m/%d/%Y"
                }
            },
            year: { 
                $year: {
                    $dateFromString: { 
                        dateString: "$StockDate", 
                        format: "%m/%d/%Y"
                    }
                }
            },
            quarter: {
                $cond: {
                    if: { $lte: [{ $month: { $dateFromString: { dateString: "$StockDate", format: "%m/%d/%Y" } } }, 3] },
                    then: 1,
                    else: {
                        $cond: {
                            if: { $lte: [{ $month: { $dateFromString: { dateString: "$StockDate", format: "%m/%d/%Y" } } }, 6] },
                            then: 2,
                            else: {
                                $cond: {
                                    if: { $lte: [{ $month: { $dateFromString: { dateString: "$StockDate", format: "%m/%d/%Y" } } }, 9] },
                                    then: 3,
                                    else: 4
                                }
                            }
                        }
                    }
                }
            }
        }
    },
    {
        $match: {
            year: 2023
        }
    },
    {
        $group: {
            _id: { year: "$year", quarter: "$quarter" },
            avg_high_price: { $avg: "$High" },
            avg_low_price: { $avg: "$Low" },
            avg_price: { 
                $avg: { $divide: [{ $add: ["$High", "$Low"] }, 2] }
            }
        }
    },
    {
        $project: {
            _id: 0,
            year: "$_id.year",
            quarter: "$_id.quarter",
            avg_high_price: { $round: ["$avg_high_price", 2] },
            avg_low_price: { $round: ["$avg_low_price", 2] },
            avg_price: { $round: ["$avg_price", 2] }
        }
    },
    {
        $sort: {
            quarter: 1
        }
    }
]);


db.sia_stock.aggregate([
    {
        $addFields: {
            stockDateConverted: {
                $dateFromString: {
                    dateString: "$StockDate",
                    format: "%m/%d/%Y"
                }
            },
            year: { 
                $year: {
                    $dateFromString: { 
                        dateString: "$StockDate", 
                        format: "%m/%d/%Y"
                    }
                }
            },
            quarter: {
                $cond: {
                    if: { $lte: [{ $month: { $dateFromString: { dateString: "$StockDate", format: "%m/%d/%Y" } } }, 3] },
                    then: 1,
                    else: {
                        $cond: {
                            if: { $lte: [{ $month: { $dateFromString: { dateString: "$StockDate", format: "%m/%d/%Y" } } }, 6] },
                            then: 2,
                            else: {
                                $cond: {
                                    if: { $lte: [{ $month: { $dateFromString: { dateString: "$StockDate", format: "%m/%d/%Y" } } }, 9] },
                                    then: 3,
                                    else: 4
                                }
                            }
                        }
                    }
                }
            }
        }
    },
    {
        $match: {
            year: 2022
        }
    },
    {
        $group: {
            _id: { year: "$year", quarter: "$quarter" },
            avg_high_price: { $avg: "$High" },
            avg_low_price: { $avg: "$Low" },
            avg_price: { 
                $avg: { $divide: [{ $add: ["$High", "$Low"] }, 2] }
            }
        }
    },
    {
        $project: {
            _id: 0,
            year: "$_id.year",
            quarter: "$_id.quarter",
            avg_high_price: { $round: ["$avg_high_price", 2] },
            avg_low_price: { $round: ["$avg_low_price", 2] },
            avg_price: { $round: ["$avg_price", 2] }
        }
    },
    {
        $sort: {
            quarter: 1
        }
    }
]);

const quarterlyData = db.sia_stock.aggregate([
    {
        $project: {
            year: { $year: { $dateFromString: { dateString: "$StockDate", format: "%m/%d/%Y" } } },
            quarter: { $ceil: { $divide: [{ $month: { $dateFromString: { dateString: "$StockDate", format: "%m/%d/%Y" } } }, 3] } },
            High: 1,
            Low: 1
        }
    },
    { $match: { year: { $in: [2022, 2023] } } },
    {
        $group: {
            _id: { year: "$year", quarter: "$quarter" },
            avg_high_price: { $avg: "$High" },
            avg_low_price: { $avg: "$Low" },
            avg_price: { $avg: { $divide: [{ $add: ["$High", "$Low"] }, 2] } }
        }
    }
]).toArray();

const compareData = quarterlyData.filter(q => q._id.year === 2023)
    .map(q2023 => {
        const q2022 = quarterlyData.find(q => q._id.year === 2022 && q._id.quarter === q2023._id.quarter);
        if (q2022) {
            return {
                year: q2023._id.year,
                quarter: q2023._id.quarter,
                qoq_high_change_percentage: ((q2023.avg_high_price - q2022.avg_high_price) / q2022.avg_high_price * 100).toFixed(2),
                qoq_low_change_percentage: ((q2023.avg_low_price - q2022.avg_low_price) / q2022.avg_low_price * 100).toFixed(2),
                qoq_avg_change_percentage: ((q2023.avg_price - q2022.avg_price) / q2022.avg_price * 100).toFixed(2)
            };
        }
    }).filter(d => d);

// Sort by ascending quarter
compareData.sort((a, b) => a.quarter - b.quarter);

console.log(compareData);

// Qn 6
db.customer_booking.aggregate([
  {
    $group: {
      _id: {
        route: "$route",
        sales_channel: "$sales_channel",
        flight_duration: "$flight_duration"
      },
      sum_flights: { $sum: 1 },
      avg_length_of_stay: { $avg: "$length_of_stay" },
      avg_flight_duration: { $avg: "$flight_duration" },
      avg_wants_extra_baggage: { $avg: "$wants_extra_baggage" },
      avg_wants_preferred_seat: { $avg: "$wants_preferred_seat" },
      avg_wants_in_flight_meals: { $avg: "$wants_in_flight_meals" }
    }
  },
  {
    $project: {
      sales_channel: "$_id.sales_channel",
      route: "$_id.route",
      flight_duration: "$_id.flight_duration",
      sum_flights: 1,
      AverageLengthOfStayPerFlightDuration: {
        $round: [
          { $divide: ["$avg_length_of_stay", "$avg_flight_duration"] },
          2
        ]
      },
      AverageBaggageRequestPerFlightDuration: {
        $round: [
          { $divide: ["$avg_wants_extra_baggage", "$avg_flight_duration"] },
          2
        ]
      },
      AveragePreferredSeatRequestPerFlightDuration: {
        $round: [
          { $divide: ["$avg_wants_preferred_seat", "$avg_flight_duration"] },
          2
        ]
      },
      AverageInFlightMealRequestPerFlightDuration: {
        $round: [
          { $divide: ["$avg_wants_in_flight_meals", "$avg_flight_duration"] },
          2
        ]
      }
    }
  },
  {
    $sort: { sum_flights: -1, "route": 1 }
  }
  ]);
  
// Qn 7 
db.airlines_reviews.aggregate([
  {
    $addFields: {
      Seasonality: {
        $cond: [
          { $in: [{ $substr: ["$MonthFlown", 0, 3] }, ["Jun", "Jul", "Aug", "Sep"]] },
          "Seasonal",
          "Non-Seasonal"
        ]
      }
    }
  },
  {
    $group: {
      _id: {
        airline: "$Airline",
        class: "$Class",
        seasonality: "$Seasonality"
      },
      AvgSeatComfort: { $avg: "$SeatComfort" },
      AvgFoodnBeverages: { $avg: "$FoodnBeverages" },
      AvgInflightEntertainment: { $avg: "$InflightEntertainment" },
      AvgValueForMoney: { $avg: "$ValueForMoney" },
      AvgOverallRating: { $avg: "$OverallRating" }
    }
  },
  {
    $project: {
      _id: 0,
      Airline: "$_id.airline",
      Class: "$_id.class",
      Seasonality: "$_id.seasonality",
      AvgSeatComfort: 1,
      AvgFoodnBeverages: 1,
      AvgInflightEntertainment: 1,
      AvgValueForMoney: 1,
      AvgOverallRating: 1
    }
  },
  {
    $addFields: {
      sortKey: {
        $switch: {
          branches: [
            { case: { $eq: ["$Class", "First Class"] }, then: 1 },
            { case: { $eq: ["$Class", "Business Class"] }, then: 2 },
            { case: { $eq: ["$Class", "Premium Economy"] }, then: 3 },
            { case: { $eq: ["$Class", "Economy Class"] }, then: 4 }
          ],
          default: 5
        }
      }
    }
  },
  {
    $sort: {
      Airline: 1,
      sortKey: 1,
      Seasonality: 1
    }
  }
]);

// Qn 8
// 8(a) Count the frequency of words to shortlist categories of complaints
db.airlines_reviews.aggregate([
  // Step 1: Match 'no' recommendations and parse reviews to categorise them into complaints
  {
    $match: {
      Recommended: 'no',
      Reviews: { $exists: true, $ne: "" }
    }
  },
  // Step 2: Split Reviews into individual words and convert to lowercase
  {
    $project: {
      words: { 
        $map: {
          input: { $split: ["$Reviews", " "] },
          as: "word",
          in: { $toLower: "$$word" }
        }
      }
    }
  },
  // Step 3: Unwind the words array to make each word a document
  {
    $unwind: "$words"
  },
  // Step 4: Group by the word and count occurrences
  {
    $group: {
      _id: "$words",
      count: { $sum: 1 }
    }
  },
  // Step 5: Sort by count in descending order
  {
    $sort: { count: -1 }
  },
  // Step 6: Project the final word and count
  {
    $project: {
      word: "$_id",
      count: 1,
      _id: 0
    }
  }
]);

//8(b) Next, we identified some words indicative of complaint categories, such as "service", "experience", "food", "delayed", "seat(s)" etc. Hence, we grouped them into different complaint categories.
db.airlines_reviews.aggregate([
  // Step 1: Classify complaints based on review content
  {
    $facet: {
      seatIssues: [
        { $match: { Reviews: { $regex: "seat|leg|uncomfortable|space", $options: "i" } } },
        { $project: { Airline: 1, TypeofTraveller: 1, Complaint: "Seat Issues", OverallRating: 1, Recommended: 1 } }
      ],
      delays: [
        { $match: { Reviews: { $regex: "delayed|cancelled|on time", $options: "i" } } },
        { $project: { Airline: 1, TypeofTraveller: 1, Complaint: "Delays", OverallRating: 1, Recommended: 1 } }
      ],
      serviceIssues: [
        { $match: { Reviews: { $regex: "service|experience|staff|communication|rude|airline", $options: "i" } } },
        { $project: { Airline: 1, TypeofTraveller: 1, Complaint: "Service Issues", OverallRating: 1, Recommended: 1 } }
      ],
      foodIssues: [
        { $match: { Reviews: { $regex: "food|meal|catering", $options: "i" } } },
        { $project: { Airline: 1, TypeofTraveller: 1, Complaint: "Food Issues", OverallRating: 1, Recommended: 1 } }
      ],
      inflightEntertainment: [
        { $match: { Reviews: { $regex: "entertainment|movie|screen|wifi", $options: "i" } } },
        { $project: { Airline: 1, TypeofTraveller: 1, Complaint: "Inflight Entertainment", OverallRating: 1, Recommended: 1 } }
      ]
    }
  },
  // Step 2: Combine all the complaint categories into a single array
  {
    $project: {
      complaints: { $concatArrays: ["$seatIssues", "$delays", "$serviceIssues", "$foodIssues", "$inflightEntertainment"] }
    }
  },
  { $unwind: "$complaints" },  // Unwind to flatten the complaint categories
  { $replaceRoot: { newRoot: "$complaints" } },  // Replace root with the complaints data

  // Step 3: Group by Airline, TypeofTraveller, and Complaint, and calculate frequency and avgComplaintRating for negative reviews
  {
    $group: {
      _id: { Airline: "$Airline", TypeofTraveller: "$TypeofTraveller", Complaint: "$Complaint" },
      Frequency: { $sum: 1 },  // Count the number of complaints for each category
      avgComplaintRating: {
        $avg: { $cond: [{ $eq: ["$Recommended", "no"] }, "$OverallRating", null] }  // Only for negative reviews
      }
    }
  },

  // Step 4: Calculate the overall average rating for each Airline and TypeofTraveller
  {
    $lookup: {
      from: "airlines_reviews",
      let: { airline: "$_id.Airline", travellerType: "$_id.TypeofTraveller" },
      pipeline: [
        { $match: { $expr: { $and: [{ $eq: ["$Airline", "$$airline"] }, { $eq: ["$TypeofTraveller", "$$travellerType"] }] } } },
        { $group: { _id: null, avgOverallRating: { $avg: "$OverallRating" } } }
      ],
      as: "avgOverallRating"
    }
  },
  { $unwind: "$avgOverallRating" },

  // Step 5: Final projection, including rounding the ratings to 4 decimal places
  {
    $project: {
      Airline: "$_id.Airline",
      TypeofTraveller: "$_id.TypeofTraveller",
      Complaint: "$_id.Complaint",
      Frequency: 1,
      avgComplaintRating: { $round: ["$avgComplaintRating", 4] },  // Round complaint rating to 4 decimal places
      avgOverallRating: { $round: ["$avgOverallRating.avgOverallRating", 4] },  // Round overall rating to 4 decimal places
      _id: 0  // Remove _id field
    }
  },

  // Step 6: Sort the output by Airline, TypeofTraveller, and Complaint
  { $sort: { Airline: 1, TypeofTraveller: 1, Complaint: 1 } }
])

// Qn 9 
// Pre-COVID vs Post-COVID ratings  
db.airlines_reviews.aggregate([
    {
        $addFields: {
            Period: {
                $let: {
                    vars: {
                        // Adjust the year by prepending "20" for 21st century dates
                        monthFlownFull: {
                            $concat: [
                                "01-",
                                { $substr: ["$MonthFlown", 0, 3] }, // Extract month (e.g., "Dec")
                                "-20", // Prepend "20" to indicate 21st century
                                { $substr: ["$MonthFlown", 4, 2] }  // Extract the last two digits of year
                            ]
                        }
                    },
                    in: {
                        $cond: [
                            { $lt: [ { $dateFromString: { dateString: "$$monthFlownFull", format: "%d-%b-%Y" } }, ISODate("2020-01-23") ] },
                            "Pre-COVID",
                            {
                                $cond: [
                                    { $gte: [ { $dateFromString: { dateString: "$$monthFlownFull", format: "%d-%b-%Y" } }, ISODate("2021-02-11") ] },
                                    "Post-COVID",
                                    "During-COVID"
                                ]
                            }
                        ]
                    }
                }
            }
        }
    },
    {
        $match: {
            $or: [
                { Period: "Pre-COVID" },
                { Period: "Post-COVID" }
            ]
        }
    },
    {
        $group: {
            _id: "$Period",
            AvgOverallRating: { $avg: "$OverallRating" },
            AvgSeatComfort: { $avg: "$SeatComfort" },
            AvgFoodnBeverages: { $avg: "$FoodnBeverages" },
            AvgInflightEntertainment: { $avg: "$InflightEntertainment" },
            AvgValueForMoney: { $avg: "$ValueForMoney" }
        }
    },
    {
        $project: {
            _id: 0,
            Period: "$_id",
            AvgOverallRating: 1,
            AvgSeatComfort: 1,
            AvgFoodnBeverages: 1,
            AvgInflightEntertainment: 1,
            AvgValueForMoney: 1
        }
    }
])  
  
// Pre-COVID vs Post-COVID ranking of complaints based on frequency  
db.airlines_reviews.aggregate([
  {
    $project: {
      ReviewDate: 1,
      Reviews: 1,
      ParsedDate: {
        $dateFromString: { dateString: "$ReviewDate", format: "%d/%m/%Y" }
      },
      ReviewCategory: {
        $cond: {
          if: { $lt: [{ $dateFromString: { dateString: "$ReviewDate", format: "%d/%m/%Y" } }, new Date("2020-01-23")] },
          then: "Pre-COVID",
          else: {
            $cond: {
              if: { $gte: [{ $dateFromString: { dateString: "$ReviewDate", format: "%d/%m/%Y" } }, new Date("2023-02-13")] },
              then: "Post-COVID",
              else: "During COVID"
            }
          }
        }
      },
      ServiceQualityIssues: {
        $cond: {
          if: { $regexMatch: { input: "$Reviews", regex: /(service|experience|staff|communication|rude|airline)/i } },
          then: 1,
          else: 0
        }
      },
      TimelinessAndOperationalIssues: {
        $cond: {
          if: { $regexMatch: { input: "$Reviews", regex: /(delayed|cancelled|ticket|check-in|boarding)/i } },
          then: 1,
          else: 0
        }
      },
      SeatingAndComfortIssues: {
        $cond: {
          if: { $regexMatch: { input: "$Reviews", regex: /(seat|uncomfortable|space)/i } },
          then: 1,
          else: 0
        }
      },
      BaggageHandlingIssues: {
        $cond: {
          if: { $regexMatch: { input: "$Reviews", regex: /(suitcase|baggage|lost)/i } },
          then: 1,
          else: 0
        }
      },
      FoodAndInflightAmenitiesIssues: {
        $cond: {
          if: { $regexMatch: { input: "$Reviews", regex: /(food|meal)/i } },
          then: 1,
          else: 0
        }
      },
      RefundsAndCompensationIssues: {
        $cond: {
          if: { $regexMatch: { input: "$Reviews", regex: /(refund|compensation)/i } },
          then: 1,
          else: 0
        }
      },
      BookingAndTicketingIssues: {
        $cond: {
          if: { $regexMatch: { input: "$Reviews", regex: /(ticket|booking|check|check-in)/i } },
          then: 1,
          else: 0
        }
      },
      FlightExperienceIssues: {
        $cond: {
          if: { $regexMatch: { input: "$Reviews", regex: /(airways|flight|boarding|communication)/i } },
          then: 1,
          else: 0
        }
      }
    }
  },
  { $match: { ReviewCategory: { $in: ["Pre-COVID", "Post-COVID"] } } },
  {
    $group: {
      _id: "$ReviewCategory",
      ServiceQualityIssues: { $sum: "$ServiceQualityIssues" },
      TimelinessAndOperationalIssues: { $sum: "$TimelinessAndOperationalIssues" },
      SeatingAndComfortIssues: { $sum: "$SeatingAndComfortIssues" },
      BaggageHandlingIssues: { $sum: "$BaggageHandlingIssues" },
      FoodAndInflightAmenitiesIssues: { $sum: "$FoodAndInflightAmenitiesIssues" },
      RefundsAndCompensationIssues: { $sum: "$RefundsAndCompensationIssues" },
      BookingAndTicketingIssues: { $sum: "$BookingAndTicketingIssues" },
      FlightExperienceIssues: { $sum: "$FlightExperienceIssues" }
    }
  },
  { $sort: { "ServiceQualityIssues": -1 } }  // Sorting by Service Quality as an example
]);

// Pre-COVID vs Post-COVID ranking of complaints based on type of traveller and frequency  
db.airlines_reviews.aggregate([
  // Stage 1: Add calculated fields for COVID_Period and ComplaintCategory
  {
    $addFields: {
      COVID_Period: {
        $cond: [
          { $lt: [{ $dateFromString: { dateString: "$ReviewDate", format: "%d/%m/%Y" } }, ISODate("2020-01-23")] },
          "Pre-COVID",
          {
            $cond: [
              { $gte: [{ $dateFromString: { dateString: "$ReviewDate", format: "%d/%m/%Y" } }, ISODate("2023-02-13")] },
              "Post-COVID",
              null
            ]
          }
        ]
      },
      ComplaintCategory: {
        $switch: {
          branches: [
            { case: { $regexMatch: { input: "$Reviews", regex: /food/i } }, then: "Food Issues" },
            { case: { $regexMatch: { input: "$Reviews", regex: /seat|uncomfortable/i } }, then: "Seat Issues" },
            { case: { $regexMatch: { input: "$Reviews", regex: /service|experience|staff/i } }, then: "Service Issues" },
            { case: { $regexMatch: { input: "$Reviews", regex: /delayed|cancelled/i } }, then: "Delays" },
            { case: { $regexMatch: { input: "$Reviews", regex: /entertainment/i } }, then: "Entertainment Issues" }
          ],
          default: "Other Issues"
        }
      }
    }
  },
  // Stage 2: Filter out irrelevant records and "Other Issues"
  {
    $match: {
      Airline: "Singapore Airlines",
      COVID_Period: { $in: ["Pre-COVID", "Post-COVID"] },
      ComplaintCategory: { $ne: "Other Issues" } // Exclude "Other Issues"
    }
  },
  // Stage 3: Group by COVID_Period, Airline, TypeofTraveller, ComplaintCategory
  {
    $group: {
      _id: {
        COVID_Period: "$COVID_Period",
        Airline: "$Airline",
        TypeofTraveller: "$TypeofTraveller",
        ComplaintCategory: "$ComplaintCategory"
      },
      Frequency: { $count: {} },
      avgComplaintRating: { $avg: "$StaffService" },
      avgOverallRating: { $avg: "$OverallRating" }
    }
  },
  // Stage 4: Format output fields
  {
    $project: {
      COVID_Period: "$_id.COVID_Period",
      Airline: "$_id.Airline",
      TypeofTraveller: "$_id.TypeofTraveller",
      ComplaintCategory: "$_id.ComplaintCategory",
      Frequency: 1,
      avgComplaintRating: { $round: ["$avgComplaintRating", 4] },
      avgOverallRating: { $round: ["$avgOverallRating", 4] }
    }
  },
  // Stage 5: Sort by fields
  {
    $sort: {
      "COVID_Period": 1,
      "Airline": 1,
      "TypeofTraveller": 1,
      "ComplaintCategory": 1
    }
  }
]);

// Qn 10: refer to Qn 8 