SELECT COUNT(DISTINCT Categories.Category_Name)
FROM Categories, Bids
WHERE Categories.ItemID = Bids.ItemID
AND Bids.Amount > 100;
