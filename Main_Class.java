import java.sql.*;
import java.util.*;
import org.json.*;
/**
 *
 * @author shivam
 */
public class Main_Class {
    public static void main(String st[]){
        try{
            Scanner sc=new Scanner(System.in);
            System.out.println("enter user id");
            String user_id=sc.next();
            System.out.println("enter subject id");
            String subject_id=sc.next();
            Proficiency.compute_proficiency(user_id,subject_id);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
