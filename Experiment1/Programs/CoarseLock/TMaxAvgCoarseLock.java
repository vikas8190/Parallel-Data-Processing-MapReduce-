/**
 * Created by vikasjanardhanan on 1/23/17.
 * COARSE-LOCK: Multi-threaded version that assigns subsets of the input String[] (or
 List<String>) for processing by separate threads. This version should also use a single
 shared accumulation data structure and can only use the single lock on the entire data
 structure. Design your program to ensure (1) correct multithreaded execution and (2)
 minimal delays by holding the lock only when absolutely necessary
 */
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/***
 * Class TMaxAvgCoarseLock's object is created once which has TMaxStationInfo that holds each stationID to list of max temperatures
 * for the same station. 10 runs of the same operations to calculate average is performed to calculate the average run time. The load time
 * from the file is ignored.Once the list of max temperatures for each stationID is accumulated parallely using threads equivalent
 * to number of availableprocessors, then its processed to calculate the average
 * and the result is stored in TMaxStationAvg. The access to the shared data structure is synchronized among the threads.
 * Only if argument is given fibonacci function is run while adding the new temperature found
 * to the stationID
 */
public class TMaxAvgCoarseLock {

    private static Runtime runtime = Runtime.getRuntime();
    private static final int THREAD_COUNT = runtime.availableProcessors() ;
    public static boolean use_fibonacci=false;
    private HashMap<String,List<Double>> TMaxStationInfo= new HashMap<String, List<Double>>();
    private HashMap<String,Double> TMaxStationAvg= new HashMap<String, Double>();


    public static void main(String[] args) throws Exception  {
        String filename="1912.csv";
        // Loads the lines in file as list of string in memory
        List<String> records = loadData(filename);
        String[] StationInfo;
        List<Double> lst=new ArrayList<Double>();
        Integer avg=0;
        List<Double> temperatures = new ArrayList<Double>();

        double TotalRunTime = 0.0;
        double minRunTime=Double.MAX_VALUE;
        double maxRunTime=Double.MIN_VALUE;

        TMaxAvgCoarseLock TMaxAvg = new TMaxAvgCoarseLock();
        // Runs 10 times to calculate average RunTime
        for(int i=0;i<10;i++) {
            //Clears the hashmap entries from the previous run.
            TMaxAvg.TMaxStationAvg.clear();
            TMaxAvg.TMaxStationInfo.clear();

            if (args.length > 0) {
                use_fibonacci = true;
            }
            final long startTime = System.currentTimeMillis();
            // Below code snippet splits the input list of records in the file into chunks and send each chunk to each thread which are
            // scheduled using ExecutorService
            int startIndex = 0;
            int endIndex = 0;
            int chunk_size = records.size() / THREAD_COUNT;
            endIndex = chunk_size - 1;
            int itercount = 1;
            ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
            while (itercount <= THREAD_COUNT) {
                itercount += 1;
                pool.execute(new TemperatureWorker(TMaxAvg.TMaxStationInfo, records.subList(startIndex, endIndex),use_fibonacci));
                startIndex = endIndex;
                if (itercount == THREAD_COUNT) {
                    endIndex = records.size() - 1;
                } else {
                    endIndex += chunk_size;
                }
            }
            pool.shutdown();
            try {
                pool.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                System.out.println("Pool interrupted!");
                System.exit(1);
            }
            TMaxAvg.calculateAverage();
            final long endTime=System.currentTimeMillis();
            long execTime=endTime-startTime;
            TotalRunTime+=execTime;
            minRunTime = Math.min(minRunTime,execTime);
            maxRunTime = Math.max(maxRunTime,execTime);
        }
        System.out.println("Average Running Time:"+TotalRunTime/10+" ms");
        System.out.println("Minimum Running Time:"+minRunTime+" ms");
        System.out.println("Maximum Running Time:"+maxRunTime+" ms");
        TMaxAvg.writeAvgValueToLog(TotalRunTime,minRunTime,maxRunTime);

    }
    /***
     * calculateAverage: void
     * Iterate over the stationInfo stored, calculate the total temperature per stationId and add the result to the tMaxStationInfoAvg
     */
    public void calculateAverage(){
        List<Double> temperatures = new ArrayList<Double>();
        Iterator it = this.TMaxStationInfo.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            temperatures = (List<Double>) pair.getValue();
            String stationID = (String) pair.getKey();
            Double total_temp = 0.0;
            for (Double temperature : temperatures) {
                total_temp += temperature;
            }

            this.TMaxStationAvg.put(stationID, total_temp / temperatures.size());

            it.remove();
        }
    }

    /***
     * writeAvgValueToLog : Double Double Double
     * Checks if logs/ directory exists, creates if doesnt exist. Then iterates over the average temperatures per StationID and adds
     * the result to file in logs directory. The filename suffix is maintainted as the timestamp when the execution happens.
     * The Average, min and max runtimes are also written to the file.
     * @param totalRunTime: Double -> Total Run time of the program among the 10 runs.
     * @param minRunTime: Double -> minimum Run time of the program among the 10 runs.
     * @param maxRunTime: Double -> Maximum Run time of the program among the 10 runs.
     */
    public void writeAvgValueToLog(double totalRunTime,double minRunTime,double maxRunTime){
        String timestamp=new SimpleDateFormat("yyyy_MM_dd_hh_mm").format( new Date() );
        String filename="TMaxAvgCoarseLock_"+timestamp+".txt";
        File directory = new File("logs");
        if(!directory.exists()){
            directory.mkdir();
        }
        File log_file=new File("logs/"+filename);
        String stationID;
        Double avgTemp;
        try{
            FileWriter fw = new FileWriter(log_file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            Iterator it = this.TMaxStationAvg.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry pair = (Map.Entry)it.next();
                stationID=pair.getKey().toString();
                avgTemp=Double.parseDouble(pair.getValue().toString());
                bw.write(stationID+"|"+avgTemp+"\n");
                it.remove();
            }
            bw.write("Average Running Time:"+totalRunTime/10+" ms\n");
            bw.write("Minimum Running Time:"+minRunTime+" ms\n");
            bw.write("Maximum Running Time:"+maxRunTime+" ms\n");
            bw.close();
        }
        catch (IOException e){
            e.printStackTrace();
            System.exit(-1);
        }


    }

    /***
     * loadData(String): Reads the supplied filename line by line and appends the result to array list and returns the same at the end
     * @param filename: String -> The filename which contains the station information which is to be read.
     * @return: List<String> -> which contains all the lines in the file which was read.
     * @throws Exception
     */
    public static List<String> loadData(String filename) throws Exception{
        List<String> records;
        records = new ArrayList<String>();
        BufferedReader br = null;

        try{
            br=new BufferedReader(new FileReader(filename));
            StringBuilder sb =  new StringBuilder();
            String line = br.readLine();
            while(line!=null){
                records.add(line);
                line=br.readLine();
            }

        }   finally {
            br.close();
        }
        return records;
    }

    /***
     * The TemperatureWorker class implements run method. It receives a chunk of station information and the common shared memory location
     * where the station information is accumulated . It iterates over these records and adds the station's max temperature information
     * whenever found into the shared memory location
     */
    private static class TemperatureWorker implements Runnable{
        private HashMap<String,List<Double>> counters;
        private List<String> dataset;
        private Boolean use_fibonacci;
        public TemperatureWorker(HashMap<String,List<Double>> counters,List<String> dataset,Boolean use_fibonacci){
            this.counters = counters;
            this.dataset = dataset;
            this.use_fibonacci=use_fibonacci;

        }

        /***
         * run: Iterates over the chunk received by this thread, for all records which are TMAX records, add the temperature for the station
         * to the list of temperatures maintained for the station. If its a new station then new list of integers is created and added
         * for the station.
         */
        @Override public void run(){
            String[] StationInfo;
            String tmax_key = new String("TMAX");
            List<Double> lst=new ArrayList<Double>();
            for(String data : dataset){
                StationInfo=data.split(",");
                if(StationInfo[2].equals(tmax_key) && StationInfo[3] != null) {
                    // the whole datastructure is synchronized so that only one one thread can have access to the data structure at a time.
                    synchronized (counters){
                        if(use_fibonacci){
                            run_fibonacci(17);
                        }
                        if(counters.containsKey(StationInfo[0])){
                            lst = counters.get(StationInfo[0]);
                            lst.add(new Double(StationInfo[3]));
                            counters.put(StationInfo[0],lst);
                        }
                        else{
                            List<Double> new_lst=new ArrayList<Double>();
                            new_lst.add(new Double(StationInfo[3]));
                            counters.put(StationInfo[0],new_lst);
                        }
                    }
                }
            }
        }

        /***
         * run_fibonacci : Executes fibonacci number generation upto the given n fibonacci numbers
         * @param n : The number of fibonacci number sequences to be calculated
         * @return : nth number in fibonacci series.
         */
        public static int run_fibonacci(int n){
            if(n==0){
                return 0;
            }
            else if(n==1){
                return 1;
            }
            else{
                return run_fibonacci(n-1)+run_fibonacci(n-2);
            }
        }


    }
}
