SELECT COUNT(DISTINCT Items.SellerID)
FROM Items, Bids
WHERE Items.SellerID = Bids.UserID;
