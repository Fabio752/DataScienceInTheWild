# HW0 - Data Science In the Wild

### How to run code:

#### ex 4.
From the root repository, you can run exercise 4 by running the command: 
```
spark-submit ./wordcount.py ./wiki.txt ./wordcount_output
```

This will generate a _wordcount_output_ folder that contains the _part-00000_ file with all the tuples (word, word_count) contained in the _wiki.txt_ input file.

The output file for this exercise can be found in the results folder under the name _wordcount_output_.

_ 

#### ex 5.
Similarly, you can run the command:
```
spark-submit ./bigram.py ./wiki.txt ./bigram_count ./bigram_frequency
```

to run exercise 5, which generates two outputs, the count of each bigram and its conditional frequency. The two outputs will be included in the _part-00000_ files included in the _bigram_count_ and _bigram_frequency_ folders respectively.

In terms of interpretability, I have decided to order the bigrams alphabetically, so that it is easy to run through them and check how many times each bigram occurs in the input file (from the _bigram_count_) and the the frequency with which that bigram appears conditionally to how many times a bigram with the same first word appears (from the _bigram_frequency_).

For example, the bigram __'a a'__ appears 335 times, however that only accounts for a 0.001375 of the bigrams that begin with the word 'a' since it is a very common word. Infact, we can use the output of ex4 to easily check the correctness of this percentage.
The wordcount output shows that the word 'a' appears 243582 times in the input file, and indeed the percentage computed above is equal to 335 divided by 243582.

On the contrary, the bigram __'aaaaaah when'__ only appears one time, but its conditional frequency is 1.0 since there are no other occurrences of different bigrams starting with the word 'aaaaaah'.

The output files for this exercise can be found in the results folder under the names:
- _bigram_count_output
- _bigram_frequency_output

### Results:
The output files for exercise 4 and 5 can be found in the following google drive folder:

_https://drive.google.com/drive/folders/1XTNX_3wrlfuUV5ff3-mZ-Ah6TqbcKRty?usp=sharing_

