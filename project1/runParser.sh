JSON_DIR="./ebay_data"
SCRIPT_PATH="./TransformationJson.py"
CATEGORY_TB="./Categories.dat"
ITEMS_TB="./Items.dat"
BIDS_TB="./Bids.dat"
USERS_TB="./EbayUsers.dat"
rm -f $CATEGORY_TB
rm -f $ITEMS_TB
rm -f $BIDS_TB
rm -f $USERS_TB
for i in $(ls $JSON_DIR); do
  python $SCRIPT_PATH $JSON_DIR/$i
done
sort -u $CATEGORY_TB -o $CATEGORY_TB
sort -u $ITEMS_TB -o $ITEMS_TB
sort -u $BIDS_TB -o $BIDS_TB
sort -u $USERS_TB -o $USERS_TB
