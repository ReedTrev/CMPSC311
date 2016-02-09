tr -cd '[a-zA-Z0-9" ""\n"]' < PangurBan.txt >  PangurBanEdit.txt
tr "\n" " " < PangurBanEdit.txt > PangurBanEdit2.txt
tr " " "\n" < PangurBanEdit2.txt > PangurBanEdit.txt
sort -f  PangurBanEdit.txt
