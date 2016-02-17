Programmers:

Alison Pastore & Trevor Reed

i)	Our shell program begins by recording the starting time for the execution. Next, it checks to see if the input file exists. If so, it removes all special characters from the input file, stores each word into its own line and sorts the list alphabetically. It then calculates a running total for the number of times each word appears and outputs it into a new text file. If the input file does not exist, it alerts the user of the error. Finally, it records the end time and calculates the total execution time by subtraction and stores it in a separate text file.

The C program, wordc.c, accepts three files as arguments. The first is the file to read from, the second is the write destination of the unique words in the read file, and the third will be written with all the run times of the program parsing the input file. Words are read one at a time from the input file and alphabetically inserted into a linked list. A count is kept for the occurences of each unique word. The list of words and their count are written to the second argument file. Runtimes are calculated for each full parsing of the input file and written to the third argument file. 

ii)	We found that in this case, shell was a lot easier to implement and more efficient. The commands did most of the work for us (sorting, counting, etc.) and allowed for a much shorter program. The C implementation took far much more time and effort and proved to be less efficient. C is overall more unwieldy compared to shell, at least in completing this particular task.

iii)	As for the shell script, the biggest problem we had was finding and using the correct syntax. The preciseness of the spacing made it difficult at times to find the source of our errors. Also, we ran into issues with the shell script on a MacBook. The timestamp didn’t seem to work properly until we switched machines.

The C program was difficult to program, yet running it gives no immediate problems.

iv)	Timing

Shell:

 Pangur Ban:

 Mean- 67954036.65 nsec

 Standard Deviation- 1966643.61298 nsec


 Hamlet:

 Mean-	434028728.65 nsec
 
 SD- 13806452.71567 nsec
 
 
 Arabian Nights:
 
 Mean-	3362910543.35 nsec
 
 SD- 12922549.31046 nsec


C:
 
 Pangur Ban:
 
 Mean- 8594.47619 usec
 
 Standard Deviation- 1236.78703 usec
 
 Hamlet:
 
 Mean- 871432.19048 usec
 
 SD- 11584.75859 usec
 
 Arabian Nights:
 
 Mean- 33681933.95238 usec

 SD- 682882.87263 usec
