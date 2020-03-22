 SELECT COUNT(DISTINCT SellerID)
FROM Items i, Bids b
WHERE i.SellerID = b.UserID;
