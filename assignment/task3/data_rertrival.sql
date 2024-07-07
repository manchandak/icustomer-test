--Total number of interactions per day
SELECT DATE(timestamp) AS date, COUNT(*) AS total_interactions
FROM user_interactions
GROUP BY DATE(timestamp)
ORDER BY date;


--Top 5 users by the number of interactions
SELECT user_id, COUNT(*) AS interaction_count
FROM user_interactions
GROUP BY user_id
ORDER BY interaction_count DESC
LIMIT 5;


--Most interacted products based on the number of interactions
SELECT product_id, COUNT(*) AS interaction_count
FROM user_interactions
GROUP BY product_id
ORDER BY interaction_count DESC;

