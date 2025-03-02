# AutoLoader Working
Here we have multiple files for various functions.
- To create a PGP file using passphrase.
- To extract a zip file and then displaying data in it.
- To extract password protected zip file and then displaying data in it.
- 
## Extraction of Zip Files
This function first copies the zip file from dbfs and then paste it into a local directory because the zip function which we are using to extract dat adoes not recognize dbfs path. After that we extract the file into a local directory and then use spark function to read that csv file and display that data. 

## Extraction of Password Protected Zip Files
In this function we are extracting a passwording protected zip files. For this we are going to use Pyzipper library. We will first copy the zip from dbfs to a local directory and then using the pyzipper function adn the password we are going to extract the file to a local directory from which we can then use the pysaprk read function to read the csv and display hte data in it.

## PGP Files
To create a PGP file we are using a library called gnupg. Firstly we will create a gnupg function then we will encrypt our file using the passphrase and then store the encrypted file with .pgp extension in some directory. Then for decrypting we just need to se the decrypt function and the passphrase then display the decrypted data on our console.
