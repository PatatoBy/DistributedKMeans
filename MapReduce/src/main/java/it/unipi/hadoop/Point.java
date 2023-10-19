package it.unipi.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Point implements Writable
{
    private int attribute_n;
    private double[] attributes = null;
    private int points_assigned;

    // Funzione che calcola la distanza euclidea tra due punti
    public static double euclideanDistance(Point a, Point b) {
        double distance = 0.0;
        for (int i = 0; i < a.getAttributes().length; i++) {
            double diff = a.getAttributes()[i] - b.getAttributes()[i];
            distance += diff * diff;
        }
        return Math.sqrt(distance);
    }

    // Costruttore utilizzato nel caso in cui in qualsiasi momento un cluster non avesse alcun punto assegnato
    public Point()
    {
        points_assigned = 0;
    }

    // Creo un punto data la dimensione. All'inizio ogni coordinata e' 0
    public Point(int d){
        attribute_n = d;
        attributes = new double[attribute_n];
        for(int i = 0; i< attribute_n; i++) attributes[i] = 0;
        points_assigned = 0;
    }

    // Creo un punto da una stringa CSV
    public Point(String s){
        String[] fields = s.split(",");

        attribute_n = fields.length;
        points_assigned = 1;
        attributes = new double[attribute_n];

        for (int i = 0; i < attribute_n; i++) {
            attributes[i] = Double.parseDouble(fields[i]);
        }
    }

    // Creo un punto da un vettore di double
    public Point(double[] src){
        attribute_n = src.length;
        attributes = new double[attribute_n];
        for(int i = 0; i<attribute_n; i++) this.attributes[i] = src[i];
    }
    
    public double[] getAttributes()
    {
        return attributes;
    }

    public double getFeature(int i){
        return attributes[i];
    }

    public void preMul(){
        for(int i = 0; i < attribute_n; i++) attributes[i] *= points_assigned; 
    }

    // this += toSum
    public void sum(Point toSum)
    {
        for (int i = 0; i < this.attribute_n; i++){
            this.attributes[i] += toSum.attributes[i] * toSum.points_assigned;

        }

        this.points_assigned += toSum.points_assigned;
    }

    public void avg()
    {
        /* Prende in input un punto che ha gia le componenti sommate, e fa la media 
         * per restituire il nuovo centroide del cluster
         * 
         * Dopo aver aver eseguito la funzione (nel Reducer teoricamente), il vettore
         * attributes contiene le coordinate del nuovo centroide del singolo cluster
         * 
         * Le coordinate del nuovo centroide andranno al rispettivo indice del vettore
         * "newCenters" nel main
         */
        for(int i = 0; i < attribute_n; i++) attributes[i] = attributes[i] / points_assigned;
    }

    // Crea un nuovo punto copiando un punto gia esistente
    public static Point copy(Point toCopy)
    {
        if(toCopy == null) return new Point();

        Point toReturn = new Point(toCopy.attributes);
        toReturn.points_assigned = toCopy.points_assigned;

        return toReturn;
    }

    /* ----------------------------------
     *        FUNZIONI OBBLIGATORIE PER INTERFACCIA WRITABLE
     * ----------------------------------
     */

    // Serializzazione
    public void write(DataOutput out) throws IOException
    {
        out.writeInt(attribute_n);
        for(int i = 0; i < attribute_n; i++) out.writeDouble(attributes[i]);
        out.writeInt(points_assigned);
    }

    // Deserializzazione
    public void readFields(DataInput in) throws IOException
    {
        attribute_n = in.readInt();
        attributes = new double[attribute_n];

        for(int i = 0; i < attribute_n; i++) attributes[i] = in.readDouble();
        points_assigned = in.readInt();
    }

    // Comparare due Punti per calcolare eps in % tra questi ultimi
    public double comparator(Point o)
    {   
        
        // Soluzione che controlla la variazione con le singole coordinate (piu' preciso quindi meno efficiente)
        double diff = 0;

        for (int i = 0; i < attribute_n; i++) {

            diff += Math.abs((this.attributes[i] - o.attributes[i]) * 100 / (Math.abs(this.attributes[i]) + 1e-9));

        }
        return (diff / attribute_n);
    }

    @Override
    public String toString()
    {
        StringBuilder toReturn = new StringBuilder();

        for(int i = 0; i < this.attribute_n; i++)
        {
            toReturn.append(attributes[i]);
            if(i < this.attribute_n - 1) toReturn.append(",");
        }

        return toReturn.toString();
    }

}
