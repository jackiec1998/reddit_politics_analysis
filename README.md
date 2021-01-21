# Deprecated
Moved to BitBucket for better large file support.

# Reddit Politics Analysis
Repository to store analysis on Reddit data pulled from Pushshift's API.

# Study Period

BEGIN: Oct. 15, 2020, 1602720000 Unix Timestamp.

END: Jan. 15, 2021, 1610668800 Unix Timestamp.

I.e. in CST.

BEGIN: Wednesday, Oct. 14, 2020 @ 7PM

END: Friday, Jan. 15, 2021 @ 6PM

In actuality, the datasets include comments up to `time.time()` at the time of pulling the comments. So there may be comments beyond the strictly defined end period that need to be removed.

Note: The API, Pushshift, was down on Nov. 11th, 2020 so there's missing data there. There might be some ways to retrieve it, but haven't gotten to it and I don't think it'll make a large difference.

# Datasets Being Maintained
- `conservative_comments.csv`
- `democrats_comments.csv`, still a work in progress. Missing Oct. comments.
