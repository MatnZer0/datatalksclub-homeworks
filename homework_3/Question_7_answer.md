Question 7. It is best practice in Big Query to always cluster your data

Answer: False

Using a clustered OR partitioned table is a best practice, but to decide between using just one or both, its necessary to take into consideration:
- If the cost of the query is unknown or known in advance.
- If the data has high or low granularity
- Whether the clusters or partitions need to be updated
- The use case of the data, such as filtering or aggregating with only one or more columns.