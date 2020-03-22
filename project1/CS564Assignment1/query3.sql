SELECT COUNT(*)
FROM (
  SELECT ItemID
  FROM Categories
  GROUP BY ItemID
  HAVING COUNT(Category_Name) = 4
);
