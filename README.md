# Esempio di pipeline ETL con AWS

## Esercizio
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
La macchina a stati è contenuta nel file cryptomachine.json. Per ogni criptovaluta sono eseguite tre step contenuti in diversi script:
