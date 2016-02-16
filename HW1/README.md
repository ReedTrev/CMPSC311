Programmers:

Ali Pastore & Trevor Reed

i)	Our shell program begins by recording the starting time for the execution. Next, it checks to see if the input file exists. If so, it removes all special characters from the input file, stores each word into its own line and sorts the list alphabetically. It then calculates a running total for the number of times each word appears and outputs it into a new text file. If the input file does not exist, it alerts the user of the error. Finally, it records the end time and calculates the total execution time by subtraction and stores it in a separate text file.

ii)	We found that in this case, shell was a lot easier to implement and more efficient. The commands did most of the work for us (sorting, counting, etc.) and allowed for a much shorter program. The C implementation took far much more time and effort and proved to be less efficient.

iii)	As for the shell script, the biggest problem we had was finding and using the correct syntax. The preciseness of the spacing made it difficult at times to find the source of our errors. Also, we ran into issues with the shell script on a MacBook. The timestamp didn’t seem to work properly until we switched machines.

iv)	Timing

Shell:

 Pangur Ban:

 Mean- 67954036.65 Ns

 Standard Deviation- 1966643.61298 Ns


 Hamlet:

 Mean-	434028728.65 Ns
 
 SD- 13806452.71567 Ns
 
 
 Arabian Nights:
 
 Mean-	3362910543.35 Ns
 
 SD- 12922549.31046 Ns


C:
 
