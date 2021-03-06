cs6675
======
This project's goal is to implement the strassen algorithm for matrix multiplication using the Hama framework.<br/>
We created 4 eclipse project during the course of our development and each one can be exported as an executable jar
file in eclipse. We assume that the matUtils project is exported as "matUtils.jar" and the other ones as "strassen.jar"
</br>here is their explanation:

## matUtils

This project was created as a toolbox to manage our data. It contains 5 diferent tools:
  
Print a matrix's values in terminal:

    hama -jar matUtils.jar read <path to matrix> <rows size> <column size>

Generate a random Matrix:

    hama -jar matUtils.jar gen <row size> <column size> <output path>

Generate a random matrix in several blocks:

    hama -jar matUtils.jar genBlock <row size> <column size> <block size> <output path> <matrix name>
  
Compare the values of 2 matrices:

    hama -jar matUtils.jar cmp <path matrix 1> <path matrix 2> <mat rows size> <mat column size>

Run an empty hama job (to evaluate the setup time on a cluster):

    hama -jar matUtils.jar hamaSetup <number of nodes>


## Strassen

In this project, the master node assigns jobs to nodes and sends information to each node on what block to retrieve in order to compute the strassen algorithm on submatrices of our input A and B

    hama -jar strassen.jar <path A> <number rows A> <number columns A> <path B> <number rows B> <number columns B> <path output> <path C> [number of tasks] [block size]

## StrassenNoSync
This project marks an improvement of the previous one since no communication between nodes is required. Based on its peer number, each node retrieves the block it needs.

    hama -jar strassenNoSync.jar <path A> <number rows A> <number columns A> <path B> <number rows B> <number columns B> <path output> [number of tasks] [block size]


## StrassenNoSyncFinal
This last optimization manages the input matrices A and B as blocks (see matUtils genBlock). By generating input matrices in the form of blocks, each peer accesses a different file and does not have to parse them as it was previously done.

    hama -jar strassenNoSyncFinal.jar <path A> <number rows A> <number columns A> <path B> <number rows B> <number columns B> <path output> <block size> [number of tasks]
