
--Indexes: Create indexes on columns that are frequently used in queries, such as timestamp, user_id, and product_id.
CREATE INDEX idx_timestamp ON user_interactions(timestamp);
CREATE INDEX idx_user_id ON user_interactions(user_id);
CREATE INDEX idx_product_id ON user_interactions(product_id);


--Partitioning: Partition the table based on the timestamp column to improve query performance for time-based queries.

CREATE TABLE user_interactions_partitioned (LIKE user_interactions INCLUDING ALL) PARTITION BY RANGE (timestamp);

CREATE TABLE user_interactions_p1 PARTITION OF user_interactions_partitioned
FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');

CREATE TABLE user_interactions_p2 PARTITION OF user_interactions_partitioned
FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');

CREATE TABLE user_interactions_p3 PARTITION OF user_interactions_partitioned
FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
