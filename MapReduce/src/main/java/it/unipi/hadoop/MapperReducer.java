package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MapperReducer
{

    /* Input key tupe - Input value type - Output key type - Output value type */
    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point>
    {

        private Point[] centroids;

        // Funzione che calcol la distanza euclidea tra due punti (indipendentemente dal numero di features)
        public static double euclideanDistance(Point a, Point b) {
            double distance = 0.0;
            for (int i = 0; i < a.getAttributes().length; i++) {
                double diff = a.getAttributes()[i] - b.getAttributes()[i];
                distance += diff * diff;
            }
            return Math.sqrt(distance);
        }

        // Funzione Setup per caricare i parametri dal Context
        public void setup(Context context)
        {
            int K = Integer.parseInt(context.getConfiguration().get("k"));
            int DIM = Integer.parseInt(context.getConfiguration().get("dimentionality"));

            centroids = new Point[K];
            for(int i = 0; i< K; i++)
                centroids[i] = new Point(context.getConfiguration().get("center_" + i));
            
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            // Il blocco try serve per evitare che dia eccezione appena viene ricevuta la prima riga
            // Contenente il nome delle features (che quindi non sono double)
            // Come conseguenza la riga attuale viene skippata senza compromettere il resto dell'esecuzione

            try{
                Point point = new Point(value.toString());
                // Calcolo la distanza tra ogni punto e i centroidi per trovare quello piu vicino al punto sotto esame
                int index = 0;
                double minDistance = Double.MAX_VALUE;
                for (int i = 0; i < centroids.length; i++) {
                    double distance = euclideanDistance(point, centroids[i]);
                    if (distance < minDistance) {
                        minDistance = distance;
                        index = i;
                    }
                }
                // Ritorno quindi il numero del cluster come chiave e il punto come valore
                context.write(new IntWritable(index), point);
            }catch(Exception e){
                e.printStackTrace();
            }

        }
    }

    /* Input key tupe - Input value type - Output key type - Output value type */
    public static class KMeansReducer extends Reducer<IntWritable, Point, IntWritable, Point>
    {

        // value (Iterable) Contiene tutti i Punti con la stessa key
        public void reduce(IntWritable key, Iterable<Point> value, Context context) throws IOException, InterruptedException
        {
            // Semplicemente sommare i punti e fare la media per calcolare il nuovo centro

            Point accumulator = Point.copy(value.iterator().next());

            // Funzione che premoltiplica il primo punto dell'iteratore per calcolare la media corretta
            accumulator.preMul();            
            /*
             * Creo un nuovo punto col primo dell'iterable
             * Sommo gli altri fino alla fine
             * poi ritorno lo stesso punto chiamando avg()
             */
            while(value.iterator().hasNext()){
                accumulator.sum(value.iterator().next());
            }
            accumulator.avg();

            
            // Restituisco il nuovo centro (key, value) -> (idCluster, Point.avg())
            context.write(key, accumulator);
        }
    }

}
