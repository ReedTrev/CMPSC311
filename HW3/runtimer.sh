a=0

while [ $a -lt  5 ]; do
	./mr-wordc PangurBan.txt PBout_1_100.txt PBtime_1_100.txt 1
	./mr-wordc PangurBan.txt PBout_2_100.txt PBtime_2_100.txt 2
	./mr-wordc PangurBan.txt PBout_4_100.txt PBtime_4_100.txt 4
	./mr-wordc PangurBan.txt PBout_8_100.txt PBtime_8_100.txt 8

	./mr-wordc Hamlet.txt HamOut_1_100.txt HamTime_1_100.txt 1
	./mr-wordc Hamlet.txt HamOut_2_100.txt HamTime_2_100.txt 2
	./mr-wordc Hamlet.txt HamOut_4_100.txt HamTime_4_100.txt 4
	./mr-wordc Hamlet.txt HamOut_8_100.txt HamTime_8_100.txt 8

	./mr-wordc Arabian.txt ArabOut_1_100.txt ArabTime_1_100.txt 1
	./mr-wordc Arabian.txt ArabOut_2_100.txt ArabTime_2_100.txt 2
	./mr-wordc Arabian.txt ArabOut_4_100.txt ArabTime_4_100.txt 4
	./mr-wordc Arabian.txt ArabOut_8_100.txt ArabTime_8_100.txt 8
	let a=a+1
done
