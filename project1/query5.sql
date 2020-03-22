SELECT COUNT(DISTINCT EbayUsers.UserID)
FROM Items, EbayUsers
WHERE Items.SellerID = EbayUsers.UserID AND EbayUsers.Rating > 1000;
