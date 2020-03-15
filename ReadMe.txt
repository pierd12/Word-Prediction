
		*    *			****			*			*			*****
		*    *			*			*			*		      *       *
		*    *			*			*			*                    *         *
		******			****			*			*                    *         *
		*    *			*			*			*                    *         *
		*    *			*			*			*		       *     *
		*    *			****			******			*****			 ****


*********************************************
*#Made By:
#mhmd mhameed 314993130
#pier damouny 316397256 
*# Application structure and steps.
*# Step 1 in detail.
*# Step 2 in detail.
*# Step 3 in detail. 
OUTPUT LINK:https://s3.console.aws.amazon.com/s3/buckets/pierd/output/?region=us-west-2&tab=overview



***************Application structure and steps****************
* The application is constructed from 3 Map_Reduce applications running one AWS EMR using 3 steps. 

* Step 1 WordCount  is counting words for each 3 tuple (w1,w2,w3) we count occurrences of (w1,w2,w3) , (w1,w2) ,(w2,w3) ,w1 ,w2 and w3 and outputed the result to output1 
 folder in s3.

* Step 2 Join is for counting all word occurrences using counter and calculating the probability for w3 to come after w1,w2 using the given equation. input is output1 and the result  outputted to output2 in s3 . 

* Step 3 Sorter is to sort the result of step 2 ,we sort w1,w2  ascending and then the probability of w3 after w1,w2 , p  descending , and then output the result to 
 output folder in s3.

* delete the output1 and ouput2 from s3 because there is no use of them after application finishes.




*************** Step 1 in detail WordCount **************

* Input the 3 google gram data-set from s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data in sequence file format with block level LZO compression and output to output1 folder in our s3 storage.
* Mapper: 
** for each (w1,w2,w3) tuple input the <key,Value> output will be <w1,<(w1,w2,w3),1>>,<w2,<(w1,w2,w3),1>>,<w3,<(w1,w2,w3),1>> ,<(w1,w2),<(w1,w2,w3),1>>,<(w2,w3), 
 <(w1,w2,w3),1>> and <(w1,w2,w3),<(w1,w2,w3),1>>, meaning will count all occurrences of sub parts of the tuples and tuple itself so we can use them in the equation.

* Combinator is for each <K,<(w1,w2,w3),1>> from the mapper we locally aggregate the occurrences of k in (w1,w2,w3) :
** we create HashMap H where key is (w1,w2,w3) and value is the sum of K occurrences where (w1,w2,w3) in its values list i,e (<K,<(w1,w2,w3),1>>).
** then for each entry e=<Key,Value> in H we output <K,e> (original K , and e = <Key,Value>) to reducer.

* Reducer : we calculate the occurrences of sub part of tuple and output them with tuple and the sum .
** the input Key K , value is List  values where each item in format <(w1,w2,w3),val> (val is number).
** we sum all the val in values List , and save it in sum variable.
** then collect the unique  3gram tuples from values List (<w1,w2,w3> ,...) and save them in List "keys".
** then for each key in keys we output <key,<K,sum>> (key is unique tuple , K is original key is  sub part of tuple key and sum is the number of its occurrences in corpus).


*************** Step 2 in detail Join **************

* Input is the output of step 1 ,its output1 folder and output to output2 folder in s3.

* we start counter c0 , that we will be shared with mapper and reducer.

* Mapper : 
** the input <Key,Value> where key is unique tuple (w1,w2,w3) and value in format <x,sum> where x is sub part of Key (w2,w3,w1w2,w2w3,w1w2w3 and not w1) and sum is amount of occurrences of x in corpus.
** in case x isnt the tuple  itself (w1,w2,w3) then we output <key,Value>  same as input.
** in case x = w1,w2,w3 then we increase c0 by 3*sum . (sum is amount of occurrences of tuple * number of word in it then we get number of word occurrences).

* Reducer :
** setup : first we get the count C0 and we save it in c0 variable.
** the input Key is tuple w1,w2,w3 and values is Values List that contains the sub part of Key (Values = (<w3,N1> ,<w2w3,N2>,<w2,c1>,<w1w2,c2>,<w1w2w3,N3>).
** we extract N1,N2,N3,c1,c2,c0 and then calculate K1,K2 and use them in equation.
** after we calculate probability p of w3 coming after w1w2 we output <(w1,w2,w3), p>. 

*************** Step 3 in detail Sorter **************
* Input is the output of step 2 which is in output folder , the output is palced in output folder in S3.

* Mapper : input is <Key,Value> where Key is unique tuple (w1,w2,w3) and value is p is probability.
** then we output <<(w1,w2),p>,w3> to reducer.

* Reducer : 
** input is <Key,Value> where key = <(w1,w2),p) and value = w3
** we implemented special comparto function for key pairs that sort w1,w2 ascending and p descending therefore in this order the keys will be sent to reducer.
** then we output <(w1,w2,w3),p) and the overall output will be sorted in the order key arrive and we get sorted output as we wanted.


******************************************************




		*****				      **            *				***
		*				     *  *          *				*   *
		*				    *    *        *				*    *
		*****				   *      *      *				*    *
		*				  *        *    *				*    *
		*				 *          *  *		 		*    *
		*****				*            ** 				*   *
 												***







          
 
