package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class KMeans
{
    /*          +-----------------------------------------------------------+
     *          |                VERIFICA CONDIZIONE DI STOP                |
     *          +-----------------------------------------------------------+
     */
    private static boolean verifyStop(Point[] newC, Point[] oldC, int K, double EPS, int MAX_ITER, int i)
    {
        // Controllo se ho raggiunto il massimo numero di iterazioni
        if (i >= MAX_ITER) return true;

        //  Per ogni cluster, comparo i vecchi e i nuovi centroidi
        for (int j = 0; j < K; j++) {

            // System.out.println(newC[j].comparator(oldC[j]));

            // Se uno dei centroidi differisce dal vecchio di piu di EPS non mi fermo
            if(newC[j].comparator(oldC[j]) > EPS)
                return false;
        }
            
        // Se in tutte le coordinate la variazione e' sotto la epsilon, allora e' tempo di fermarsi
        return true;
    }

    /*          +-----------------------------------------------------------+
     *          |                NUMERO DI RIGHE DL FILE CSV                |
     *          +-----------------------------------------------------------+
     */
    private static int getDatasetSize(Configuration conf, String INPUT_FILE) throws IOException
    {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(INPUT_FILE);
        int count = 0;
        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(fs.open(path)))) {
            while (reader.readLine() != null) {

                count++;
            }
        }

        fs.close();

        return count;
    }

    /*          +-----------------------------------------------------------+
     *          |                  GENERA K CENTRI CASUALI                  |
     *          +-----------------------------------------------------------+
     */
    private static Point[]
    generateRandomCentroids(int K, int DIM, String INPUT_FILE, Configuration conf) throws IOException
    {
        Point[] toReturn = new Point[K];
        Random random = new Random();

        List<Integer> uniqueNumbers = new ArrayList<Integer>();

        int pick;
        int dataSetSize = getDatasetSize(conf, INPUT_FILE);

        while(uniqueNumbers.size() < K) {
            // Siccome prendo un CSV, la linea del nome delle colonne va scartata
            pick = random.nextInt(dataSetSize - 1) + 1;
            // Pick sara un numero da 1 a n-1 dove n e' il numero delle righe del file CSV
            if(!uniqueNumbers.contains(pick)) {
                uniqueNumbers.add(pick);
            }
        }
        
        Path path = new Path(INPUT_FILE);
    	FileSystem hdfs = FileSystem.get(conf);
        
        // Skippo la prima riga che contiene il nome delle features
        int currentLine = 1;
        String line;
        
        int i = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(path))))
        {   
            // Finche una linea esiste vado avanti
            while((line = reader.readLine()) != null) {
                
                // Se la linea corrente e' nel vettore random, lo prendo
                if (uniqueNumbers.contains(currentLine)) {
                    
                    toReturn[i] = new Point(line);
                    i++;
                    
                    if(i == K) return toReturn;
                }
                
                currentLine++;
            }
        }
        
        return toReturn;
    }
    
    /*          +-----------------------------------------------------------+
     *          |                SCRIVI CENTROIDI IN UN FILE                |
     *          +-----------------------------------------------------------+
     */
    private static void writeFile(Configuration conf, Point[] centroids, String output)
    throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream outstream = hdfs.create(new Path(output), true);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(outstream));

        // Write centroid
        for(int i = 0; i < centroids.length; i++) {
            br.write(centroids[i].toString());
            br.newLine();
        }

        br.close();
        hdfs.close();
    }
    /*          +-----------------------------------------------------------+
     *          |                     SCRIVI IN UN FILE                     |
     *          +-----------------------------------------------------------+
     */
    // Viene utilizzato per scrivere stringhe (args[]) in un file
    private static void writeInfo(Configuration conf, String output, String[] args)
    throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream outstream = hdfs.create(new Path(output), true);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(outstream));

        // Write centroid
        for(int i = 0; i < args.length; i++) {
            br.write(args[i]);
            br.newLine();
        }

        br.close();
        hdfs.close();
    }
    /*          +-----------------------------------------------------------+
     *          |              RECUPERA I K CENTROIDI PARZIALI              |
     *          +-----------------------------------------------------------+
     */
    // Recupera dai file di iterazione i centroidi restituiti dal reducer dell'iterazione precedente
    private static Point[] recoverResults(int K, String OUT_FILE, Configuration conf) throws IOException
    {
        Point[] toReturn = new Point[K];

        Path path = new Path(OUT_FILE);
        FileSystem hdfs = FileSystem.get(conf);

        FileStatus[] status = hdfs.listStatus(path);
        
        String centroidString = "";
        String line;

        // status contiene una lista con tutti i file nella cartella /iteration-X

        // Check _SUCCESS
        if(!status[0].getPath().getName().startsWith("_SUCCESS")){
            System.err.println("Error occurred at regovery partial files");
            System.exit(1);
        }

        // Parto dal secondo fil perche il primo mi dice se ho avuto successo oppure se ho fallito
        for (int i = 1; i < status.length; i++) {

            Path file = status[i].getPath();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(file)))) {

                while ((line = reader.readLine()) != null){
                    centroidString += line.split("\t")[1] + ";";
                }
            }
        }

        String[] centroidList = centroidString.split(";");

        for (int j = 0; j < K; j++) {
            toReturn[j] = new Point(centroidList[j]);
        }

        return toReturn;
    }

    /*          +-----------------------------------------------------------+
     *          |                           MAIN                            |
     *          +-----------------------------------------------------------+
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
    {

        // Config XML file contenente (EPS, MAX_ITER, K)
        Configuration conf = new Configuration();
        conf.addResource(new Path("config.xml"));

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2)
        {
            System.err.println("Usage: kmeans <input> <output>");
            System.exit(1);
        }

        // Nomi file da prendere in input e output
        String INPUT_FILE = otherArgs[0];
        String OUTPUT_FILE = otherArgs[1];

        // Vari parametri presi dal file di configurazione
        int DIM = Integer.parseInt(conf.get("dimentionality"));
        double EPS = Double.parseDouble(conf.get("eps"));
        int MAX_ITER = Integer.parseInt(conf.get("max_iter"));
        int K = Integer.parseInt(conf.get("k"));

        // Centroidi nuovi e vecchi
        Point[] newCenters = new Point[K];
        Point[] oldCenters = new Point[K];

        // Timer per il tempo richiesto per generare i primi K centroidi casuali
        double startR, endR;

        // Generare i primi newCenters a caso
        startR = System.currentTimeMillis();
        newCenters = generateRandomCentroids(K, DIM, INPUT_FILE, conf);
        endR = System.currentTimeMillis();

        // Li metto in un file per confrontarli successivamente con i finali
        writeFile(conf, newCenters, OUTPUT_FILE+"/initial.txt");

        // stopCondition
        boolean stop = false;
        // Se il job ha finito correttamente o no
        boolean succeded = true;

        // Variabili per il tempo totale di esecuzione di kmeans
        double start, end;

        int i = 0;
        String iterationOutputPath = "";
        start = System.currentTimeMillis();

        while(!stop) {
            i++;
            iterationOutputPath = OUTPUT_FILE + "/iteration-" + i;

            // Passo i centroidi ai mapper
            for(int j = 0; j<K; j++) conf.set("center_"+j, newCenters[j].toString());

            Job job = new Job(conf, "job-" + i);

            // Configurazione del job
            job.setJarByClass(KMeans.class);

            job.setMapperClass(MapperReducer.KMeansMapper.class);
            job.setCombinerClass(MapperReducer.KMeansReducer.class);
            job.setReducerClass(MapperReducer.KMeansReducer.class);  

            // Un reducer per ogni cluster
            job.setNumReduceTasks(K);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Point.class);

            FileInputFormat.addInputPath(job, new Path(INPUT_FILE));
            FileOutputFormat.setOutputPath(job, new Path(iterationOutputPath));

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            succeded = job.waitForCompletion(true);

            if(!succeded){
                System.err.println("Error at iteration "+i);
                System.exit(2);
            }

            // Sposto newCentroids in oldCenters
            for(int j = 0; j < K; j++) oldCenters[j] = Point.copy(newCenters[j]);
            // Dopodiche recupero newCenters dal file risultato dell'iterazione corrente
            newCenters = recoverResults(K, iterationOutputPath, conf);

            // System.out.println("---OLD---");
            // for(Point p : oldCenters) System.out.println(p);
            // System.out.println("---NEW---");
            // for(Point p : newCenters) System.out.println(p);

            // Verifico condizione di stop
            stop = verifyStop(newCenters, oldCenters, K, EPS, MAX_ITER, i);

        }
        end = System.currentTimeMillis();
        
        // Scrivo i centroidi finali in un file di testo
        writeFile(conf, newCenters, OUTPUT_FILE+"/final.txt");

        // Stampo altre informazioni su un file di testo
        String[] infos = {
            "Total Hadoop time -> " + ((end - start)/1000) + " s",
            "Centers generation time -> "+ ((endR-startR)/1000) + " s",
            "Total iterations -> " + i
        };
        writeInfo(conf, OUTPUT_FILE+"/info.txt", infos);
    }
}
