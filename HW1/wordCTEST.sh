input=>PangurBan.txt

edit=tr -cd '[a-zA-Z0-9" ""\n"]' | tr "\n" " " | tr " " "\n" | sort -f $input
