
import org.apache.spark.sql.*;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class Main_Class {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        
        
         
        String path = "src/main/resources/data/Wuzzuf_Jobs.csv";
        Wuzzuf_DAO job = new Wuzzuf_DAO();
        
        //1- Read data set and convert it to dataframe or Spark RDD and display some from it.
        Dataset<Row> JobsDF = job.ReadCSV(path);
        System.out.println("1- Wuzzuf Dataframe \n");
        System.out.println ("========================================================================");
        //job.PrintDF(JobsDF, 10);
        
        //2- Display structure and summary of the data.
        
        System.out.println("2-Display structure and summary of Wuzzuf Dataframe \n");
        System.out.println ("========================================================================");
        //job.PrintSummary(JobsDF);
        
        //3- Clean the data (null, duplications)
        System.out.println("3- Clean the data (null, duplications)  \n");
        System.out.println ("========================================================================");
        //before removing Duplicates and Nulls
        System.out.println("Number of rows before removing Duplicates and Nulls : "+JobsDF.count()); 
        Dataset<Row> JobsDF_clean = job.clean_data(JobsDF);
        //After removing Duplicates 
        System.out.println("Number of rows after removing Duplicates and Nulls : "+JobsDF_clean.count());
        System.out.println ("========================================================================");
        
        
        //4-Count the jobs for each company and display that in order (What are the most demanding companies for jobs?)
        System.out.println("4-Count the jobs for each company and display that in order (What are the most demanding companies for jobs?))  \n");
        System.out.println ("========================================================================");
        
        final SparkSession sparkSession = SparkSession.builder ().appName ("Spark CSV Analysis Demo").master ("local[2]")
                    .getOrCreate ();
        
        Dataset<Row> company_by_title =job.group_by_column(JobsDF ,"Company","Title" , sparkSession);
        //job.PrintDF(company_by_title,10);
        
        //5- Show step 4 in a pie chart
        System.out.println("5-Show step 4 in a pie chart \n  ");
        System.out.println ("========================================================================");

        job.pie_chart(company_by_title,10,"Number of titles for each company \n ");
        System.out.println ("========================================================================");
         
        
        //6- Find out What are it the most popular job titles
        System.out.println("6- Find out What are it the most popular job titles? \n  ");
        System.out.println ("========================================================================");
                  
         
        Dataset<Row> count_by_title =job.group_by_column(JobsDF ,"Title","Title" , sparkSession);
       // job.PrintDF(count_by_title,10);

        //7- Show step 6 in bar chart
        System.out.println("7-Show step 6 in bar chart \n  ");
        System.out.println ("========================================================================");

        //job.bar_chart(count_by_title,  10 ,  "Title","Title","Job Title ", "Number of Titles","Job Tiitles"  );
                  
        
        //8- Find out the most popular areas?
        System.out.println("8- Find out the most popular areas?  \n ");
        System.out.println ("========================================================================");
                  
         
        Dataset<Row> count_by_area =job.group_by_column(JobsDF ,"Location","Location" , sparkSession);
        //job.PrintDF(count_by_area,10);

        //9- Show step 8 in bar chart

        System.out.println("9- Show step 8 in bar chart \n  ");
        System.out.println ("========================================================================");

        job.bar_chart(count_by_area,  10 ,  "Location","Location","Locations ", "Most Popular","Locations"  );
        System.out.println ("========================================================================");
        
        //11- Factorize the YearsExp feature and convert it to numbers in new col.
        System.out.println("11- Factorize the YearsExp feature and convert it to numbers in new col. \n ");
        System.out.println ("========================================================================");
        
        Dataset<Row> factorized_jobs = job.Factorize(JobsDF);
        factorized_jobs.show();
        
        //12- Apply K-means for job title and companies
        System.out.println("12- Apply K-means for job title and companies \n ");
        System.out.println ("========================================================================");

        Dataset<Row> cluster = job.Kmeans(factorized_jobs);
        cluster = cluster.drop("features");
        Dataset<Row> newDs = cluster.withColumn("skillsindex", cluster.col("Skills"));

        cluster=newDs.drop("Skills");

        cluster.show();
        
        //10-Print skills one by one and how many each repeated and order the output to find out the most important skills required?
        System.out.println("10- Print skills one by one and how many each repeated  \n ");
        System.out.println ("========================================================================");

        List<Map.Entry>  sorted=job.PopularSkills(path);

        List<String> Keys = new ArrayList<String>();
        List<Long> value = new ArrayList<Long>();



        // DISPLAY
        for (Map.Entry<String, Long> entry : sorted) {
            Keys.add(entry.getKey ());
            value.add(entry.getValue ());
        }
        Dataset<String> popular_skills = sparkSession.createDataset(Keys, Encoders.STRING());

        popular_skills.show();
    }
    
}
