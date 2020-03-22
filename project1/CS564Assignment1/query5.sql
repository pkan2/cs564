 SELECT COUNT(DISTINCT eu.UserID)
FROM Items i, EbayUsers eu
WHERE i.SellerID = eu.UserID AND eu.Rating > 1000;
