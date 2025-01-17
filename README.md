# Esempio di pipeline ETL con AWS

## Problema
Si vuole sviluppare una pipeline per un cliente che desidera eseguire analisi su file di criptovalute, con tecnologia AWS. Le azioni richieste sono:
* caricamento dei file su S3
* trasformazione dei dati
* upload dei dati su redshift
* verificare la disponibilità dei dati per ulteriori analisi (qury SQL/software di BI)

Per ogni valuta da caricare sono forniti due files: Storico.csv e Trend.csv (con nomi diversi e non standardizzati per ogni valuta). Il file Storico.csv contiene le colonne Data e Price, da caricare, con missing value pari a -1. La cadenza
è giornaliera. Il file Trend.csv contiene il google trend settimanale. Nella richiesta si pone l’accento sul parallelismo e la scalabilit`a dell’esecuzione della pipeline, che deve essere indipendente per ogni valuta. I file sono forniti dall’utente che li carica su S3.

## Soluzione
### Script
La soluzione proposta è una macchina a stati implementata con step functions che richiama componenti ETL glue.
#### Caricamento dati
Lo script **load_raw_files.py** carica su S3 i files presenti nella directory in cui è lanciato. Le informazioni di autenticazione devono essere contenute nel file secret.key a parte.
#### Step functions
La macchina a stati è contenuta nel file **cryptomachine.json**. 
![figura1](https://github.com/LucaTrussoni/an_AWS_pipeline/blob/b4e545dfa049ead17204d670be8e21085aebc050/cryptomachine.png)

Per ogni criptovaluta sono eseguite tre step contenuti in diversi script:
* lo script **bronze2silver.py** porta i dati dal bucket raw al bucket argento, occupandosi di gestire i dati mancanti. Lo script riceve i parametri HIST_FILE (nome del file contenente le quotazioni
storiche), TREND_FILE (nome del file contenente il google trend), TICKER  (stringa di tre caratteri contenente il ticker della valuta), DROP (vale Y o N a seconda che si voglia cancellare o riempire
con l’ultimo prezzo valido un valore mancante nel file delle quotazioni storiche). Lo script popola il bucket argento con i percorsi QQQ_hist e QQQ_trend, ove QQQ `e il ticker della valuta, contenente i file integrati in formato parquet.
* lo script **silver2gold.py** trasporta i dati dal bucket argento al bucket oro, allineando temporalmente i valori dei file con i prezzi storici e le rilavazioni del google trend e aggiungendo una media mobile dei prezzi e un’interplazione lineare per date del google trend. Lo script riceve il parametro TICKER (il ticker della valuta). Lo script popola il bucket oro con il percorso QQQ, cio`e il ticker della valuta, con
un file parquet contenente i dati opportunamente elaborati
* lo script **RedshiftUpload.py** si occupa di caricare i dati su redshift (nel database crypto). Lo script riceve il parametro TICKER (ticker della valuta). Lo script carica sul database crypto in un gruppo di lavoro redshift, sfruttando la connessione crypto-connection. Nel database deve essere presente lo schema market_data che viene popolato con una tabella con nome uguale al ticker. Se la tabella è presente essa viene sostituita con quella nuova.

Prima dell'esecuzione lo script **ClearBuckets.py** si occupa di cancellare i contenuti dei bucket argeto e oro.

#### Esecuzione
Gli script sono eseguiti con il ruolo MyETLGlue che contiene le policy AmazonDMSRedshiftS3Role, AmazonRedshiftAllCommandsFullAccess, AmazonS3FullAccess e AWSGlueServiceRole (anche se sarebbe meglio adottare policy più restrittive). In AWS i nodi paralleli delle macchine a stati sono configurati in modo da bloccarsi non appena fallisce
uno qualsiasi dei processi paralleli, fermando il flusso delle altre pipeline che potrebbero invece procedere indisturbati. Per ovviare
a questo comportamento l’eventuale failure della pipeline di ciascuna valuta è gestita mediante un nodo ”Pass” che intercetta eventuali problemi: se si verifica il fallimento di una pipeline, le altre procedono senza che che il processo parallelo fallisca nel suo complesso. In figura un esempio del flusso di esecuzione in cui si è provocato artificialmente un errore nella pipeline che gestisce Monero:
![figura2](https://github.com/LucaTrussoni/an_AWS_pipeline/blob/effa42126f6c66c991e2997ef1ed5976ed420a1b/error_mngmt.png)
l’esecuzione non termina verso la failure del processo parallelo bloccando la pipeline bitcoin ma procede grazie all’attivazione del nodo pass.
Una volta eseguita la pipeline ecco come appaiono i dati dentro redshift:

![figura3](https://github.com/LucaTrussoni/an_AWS_pipeline/blob/effa42126f6c66c991e2997ef1ed5976ed420a1b/Redshift-caricamento-btc.png)
