# Airline Passenger Analytics

## Overview
This project uses real-world review data to uncover how seasonality, passenger type, and travel class affect satisfaction across key airline service features. These insights will empower airlines to deliver targeted improvements that elevate passenger experience and loyalty.

## Project Goals
- Compare customer satisfaction across **seasonal vs. non-seasonal** travel
- Benchmark service quality of **Qatar Airways vs. Singapore Airlines**
- Classify and analyse **complaints by traveller type**
- Provide **data-driven recommendations** to improve airline operations

## Methodology
- **Seasonal Tagging**: Grouped records by `MonthFlown` into seasonal vs. non-seasonal  
- **Aggregated Ratings**: Computed average feature ratings by `Airline`, `Class`, and `Period`  
- **Complaint Extraction**: Filtered negative reviews (`Recommended = No`)  
- **Keyword Analysis**: Identified complaint themes (eg. Seat, Food, Service, Delay)  
- **Visualisation**: Used bar charts and grouped plots for interpretability  
- **Benchmarking**: Compared ratings by traveller type and class  

## Key Insights

### (a) Seasonality Matters
- Non-seasonal periods see **higher satisfaction** across all features
- Fewer crowds → better service → higher perceived value

### (b) Airline & Class Performance
- **Qatar Airways**: More consistent experience across all classes  
- **Singapore Airlines**: Strong First Class, but **Premium Economy underwhelms**  
- Satisfaction generally drops from First → Business → Economy (industry norm)

### (c) Top Complaint Themes
- Common complaints were regarding **Service Quality**, **Seating**, and **Entertainment**  
- **Solo leisure travellers** complain the most; **business travellers** the least 
- **Group travellers** (eg. families) report **lower ratings** for service & entertainment

### Overall Ratings
- Qatar Airways consistently scores higher (6.48–7.61)  
- Singapore Airlines scores are more variable and dip below 6.5 for some segments

## Recommendations
| Area | Qatar Airways | Singapore Airlines |
|--------|------------------|-------------------------|
| **Delays** | Streamline operations (eg. baggage handling) | Reduce delay-related friction across all passenger types |
| **Inflight Entertainment** | Offer more content for families (eg. children’s shows) | Tailor offerings for business travellers (e.g. news, podcasts) |
| **Seating Comfort** | Improve layout for families/couples | Add legroom for family travellers |
| **Service Quality** | Train crew for group-oriented service delivery | Gather direct feedback from business travellers |
| **Business Travellers** | Maintain good service & reduce delay variance | Introduce productivity features (eg. adjustable workspace, Wi-Fi) |

## Impact
- Helps airlines **target weak spots** in service delivery
- Drives **data-informed resource planning** during peak periods
- Supports **customer segmentation strategies** by traveller profile
- Promotes **proactive complaint resolution** for long-term loyalty
