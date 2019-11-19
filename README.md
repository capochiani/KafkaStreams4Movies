# KafkaStreams4Movies


Kafka Streams for Movies.

In questo repository, vengono caricate 4 classi Java.
 
StreamsCountMovieTopic.java (1) implementa una classe StreamsCountMovieTopic (Streams DSL) che legge dal
topic "streams-movie-input" i cui valori del messaggio rappresentano linee di testo e 
splitta ogni linea in parole per poi scriverle in un sink topic "streams-countmovieTopic-output".
 
StreamsCountMovie.java (2) implementa una classe (via Streams DSL) che legge dal topic "streams-movie-input" 
i cui valori del messaggio rappresentano linee di testo. Splitta ogni linea in parole e poi conta le 
occorrenze (o altre funzioni di count sum,min,max,avg...) che vengo scritte nel topic "streams-countmovie-output".
 
Le classi KStreamsCountMovie.java (3) e KStreamsCountMovieVar.java (4)
eseguono gli stessi script, ma differiscono per sintassi.
Nel secondo caso (4) infatti, tutte le variabili di accesso vengono dichiarate esternamente.

_________________
Note - da console:

  Per creare topic movie-input & movie-output:
   > stream topic create -path /movie-stream -topic movie-input 
    
 Per lanciare la classe KStreamsCountMovie:  
    > java -cp "$(mapr clientclasspath):<Application Name>.jar" KStreamsCountMovie

Francesca
