import java.sql.*;
import java.util.*;
import org.json.*;
import org.postgresql.Driver;
/**
 *
 * @author shivam
 */
public class Proficiency {
    // function to print results in csv files.

    // function to make dictionary for list of all competencies in a grade domain
    // ex : {[g1, d1] : [c1, c3, c4]}


    static Connection getConnection(String connection_string, String username, String password){
        try {
            //check driver and create a connection
            Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection(connection_string,username,password);
            return connection;
        }
        catch(Exception e){
            //return null if connection attempt is failed
            e.printStackTrace();
            return null;
        }
    }
    static ResultSet getResultSet_Execute(String url,Connection connection) throws SQLException {
        //return result set of select query
        Statement stmt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
        //PreparedStatement pstmt1 = connection.prepareStatement(url);
        //org.postgresql.PGStatement pgstmt1 = (org.postgresql.PGStatement) pstmt1;
        ResultSet rs = stmt.executeQuery(url);
        return rs;
    }
    static ArrayList get_lscores(JSONArray C,ResultSet rs_all_user_sub,String user_id,double user_badge,Connection connection1, ArrayList <String> allowed_competencies)  throws JSONException, SQLException {
        ArrayList lscores = new ArrayList();
        //Run a loop for each concept node earned by user,compute percentile and add it to lscores
        for (int c = 0; c < C.length(); c++) {

        	if(allowed_competencies.contains((String)C.getJSONObject(c).get("competency_id"))) {

	            String current_status=(String) C.getJSONObject(c).get("status");
	            if(current_status.equals("completed")){
	            	//System.out.println( ((String) C.getJSONObject(c).get("competency_id")) + '\n');
	                String current_node = (String) C.getJSONObject(c).get("competency_id");
	                //create empty hashtable badges
	                HashMap<String, Integer> badges = new HashMap<String, Integer>();
	                badges.put("0.20", 0);
	                badges.put("0.40", 0);
	                badges.put("0.60", 0);
	                badges.put("0.80", 0);
	                badges.put("1.00", 0);
	                //create empty list to hold all badges earned by any learner in c
	                ArrayList B = new ArrayList();
	                //get badge earned by user_id in current concept node c and store it in user_badge
	                JSONArray temp=C;
	                for (int t = 0; t < temp.length(); t++) {
	                	if(allowed_competencies.contains( (String)temp.getJSONObject(t).get("competency_id") )) {
		                    String status=(String) temp.getJSONObject(t).getString("status");
		                    if(status.equals("completed")) {
		                        String t_node = (String) temp.getJSONObject(t).getString("competency_id");
		                        String t_badge = (String) temp.getJSONObject(t).getString("badge");
		                        if (t_node.equals(current_node)) {
		                            String[] tok = t_badge.split("-", 2);
		                            String t_badge_split = tok[1];
		                            //this stores user_badge in competency c and break this loop
		                            user_badge = Double.parseDouble(t_badge_split);
		                            break;
		                        }
		                    }
	                	}
	                }
	                ResultSet rs2 = rs_all_user_sub;
	                rs2.beforeFirst();
	                int Blc_base = 0;
	                while (rs2.next()) {
	                    JSONArray temp2 = new JSONArray(rs2.getString(1));
	                    for (int t = 0; t < temp2.length(); t++) {
	                    	if(allowed_competencies.contains( (String)temp2.getJSONObject(t).get("competency_id"))) {
		                        String temp_status = (String) temp2.getJSONObject(t).getString("status");
		                        //if status is completed then only consider badge
		                        if (temp_status.equals("completed")) {
		                            String t_node = (String) temp2.getJSONObject(t).getString("competency_id");
		                            String t_badge = (String) temp2.getJSONObject(t).getString("badge");
		                            if (t_node.equals(current_node)) {
		                                String[] tok = t_badge.split("-", 2);
		                                String t_badge_split = tok[1];
		                                int val = badges.get(t_badge_split);
		                                val++;
		                                badges.put(t_badge_split, val);
		                            }
		                        }
	                    	}
	                    }
	                }
	                if (user_badge >= 0.2)
	                    Blc_base = Blc_base + badges.get("0.20");
	                if (user_badge >= 0.4)
	                    Blc_base = Blc_base + badges.get("0.40");
	                if (user_badge >= 0.6)
	                    Blc_base = Blc_base + badges.get("0.60");
	                if (user_badge >= 0.8)
	                    Blc_base = Blc_base + badges.get("0.80");
	                if (user_badge == 1.0)
	                    Blc_base = Blc_base + badges.get("1.00");

	                int Badges_in_c = badges.get("0.20") + badges.get("0.40") + badges.get("0.60") + badges.get("0.80") + badges.get("1.00");
	                double percentile = (Blc_base * 100) / Badges_in_c;
	                lscores.add(percentile);

	            }
        	}

        }//for c in C loop ends here
        return lscores;
    }
    static JSONObject getBadge(ArrayList lscores,ResultSet rs) throws SQLException, JSONException {
        LinkedList AllBadgesList = new LinkedList();
        while (rs.next()) {
            JSONArray temp=new JSONArray(rs.getString(1));
            for (int t = 0; t < temp.length(); t++) {
                String status=(String) temp.getJSONObject(t).getString("status");
                if(status.equals("completed")) {
                    String t_badge = (String) temp.getJSONObject(t).getString("badge");
                    String[] tok = t_badge.split("-", 2);
                    String t_badge_split = tok[1];
                    double badge_split = Double.parseDouble(t_badge_split);
                    AllBadgesList.add(badge_split);
                }
            }
        }
        double lscores_sum = 0.0;
        for (Iterator it = lscores.iterator(); it.hasNext(); ) {
            double lscores_iter = (double) it.next();
            lscores_sum += lscores_iter;
        }
        double my_lscore = lscores_sum / lscores.size();
        Collections.sort(AllBadgesList);
        //calculate index of badge whose range is between 0 and AllBadgeList size
        double my_index_temp = Math.ceil((my_lscore * AllBadgesList.size()) / 100);
        int my_index = (int) my_index_temp;
        my_index--;
        JSONObject myprof = new JSONObject();
        double myprof_bucket = 0.00;
        if (my_index >= 0 && my_index < AllBadgesList.size()) {
            myprof_bucket = (double) AllBadgesList.get(my_index);
        }
        //set learner proficiency badge according to value present at AllBadgeList(index)
        if (myprof_bucket == 0.20)
            myprof.put("badge","0.00-0.20");
        else if (myprof_bucket == 0.40)
            myprof.put("badge","0.20-0.40");
        else if (myprof_bucket == 0.60)
            myprof.put("badge","0.40-0.60");
        else if (myprof_bucket == 0.80)
            myprof.put("badge","0.60-0.80");
        else if (myprof_bucket == 1.00)
            myprof.put("badge","0.80-1.00");
        return myprof;
    }
    static void set_proficiency(String learner_id, String subject_id,JSONObject proficiency) throws SQLException, JSONException {
        //connect to nucleus database deepend_lbt
        //Connection connection=Proficiency.getConnection("jdbc:postgresql://localhost:5432/postgres","postgres", "complicated");
	    Connection connection = Proficiency.getConnection("jdbc:postgresql://postgres-nucleusdb.internal.gooru.org/nucleus","goorulabs", "Maxmin123");
        if(connection!=null){
            Statement stmt = connection.createStatement();
            //update proficiency of learner
            String url = "update deepend_lbt set proficiency='" + proficiency + "' where user_id='" + learner_id + "' and subject_id='"+subject_id+"'";
            //stmt.executeUpdate(url);
        }
    }
    static JSONObject get_proficiency(String learner_id,String subject_id) throws SQLException, JSONException {
        //connect to nucleus database deepend_lbt
        //Connection connection=Proficiency.getConnection("jdbc:postgresql://localhost:5432/postgres","postgres", "complicated");
	    Connection connection = Proficiency.getConnection("jdbc:postgresql://postgres-nucleusdb.internal.gooru.org/nucleus","goorulabs", "Maxmin123");
        if (connection != null) {
            //get proficiency of learner and return it
            PreparedStatement pstmt1 = connection.prepareStatement("select proficiency from deepend_lbt where user_id='" + learner_id + "' and subject_id='"+subject_id+"'");
            org.postgresql.PGStatement pgstmt1 = (org.postgresql.PGStatement) pstmt1;
            ResultSet rs = ((PreparedStatement) pgstmt1).executeQuery();
            JSONObject user_proficiency=new JSONObject();
            while (rs.next()) {
                user_proficiency =new JSONObject(rs.getString(1));
            }
            return user_proficiency;
        }
        else
            return null;
    }
    static void compute_proficiency(String user_id,String subject_id) throws SQLException, JSONException, ClassNotFoundException {
        //connect to nucleusdb and create a connection object
        Connection connection1=Proficiency.getConnection("jdbc:postgresql://postgres-nucleusdb.internal.gooru.org:5432/nucleus","goorulabs", "Maxmin123");
        //Connection connection1=Proficiency.getConnection("jdbc:postgresql://localhost:5432/postgres","postgres", "complicated");
        if(connection1!=null) {
            //initialise user_badge for every learner
            double user_badge=0.0;
            //badge declaration
            JSONObject badge=new JSONObject();

            //get comptency_list of user in subject
            ResultSet learning_map = Proficiency.getResultSet_Execute("select learning_map from deepend_lmbt where learningmap_id = '" + subject_id + "'" ,connection1);
            learning_map.next();
            //System.out.println(learning_map.getString(1));
            JSONArray learning_map_data = new JSONArray(learning_map.getString(1));
            HashMap<ArrayList<String>, ArrayList<String> > grade_domain_mapping = new HashMap<ArrayList<String>, ArrayList<String> > ();
            for (int i = 0; i < learning_map_data.length(); i++) {
                JSONObject competency = learning_map_data.getJSONObject(i);
                ArrayList<String> keyStr = new ArrayList<String>();
                keyStr.add(competency.getString("tx_course_code"));
                keyStr.add(competency.getString("tx_domain_code"));
                ArrayList<String> valueArr = new ArrayList<String>();

                if (grade_domain_mapping.containsKey(keyStr)) {
                    valueArr = grade_domain_mapping.get(keyStr);
                }
                valueArr.add(competency.getString("tx_comp_code"));
                grade_domain_mapping.put(keyStr, valueArr);

            }
            // ---- --- -----

            // Here we will have a map from grade domain to all competencies by calling GD_comp mapping function.

            // --- - --- ----


            // loop starts here for each grade domain
            // now sql will only return the competency_list with competencies in the given grade domain.
            ResultSet rs_user_sub = Proficiency.getResultSet_Execute("select progress->'competency_list' from deepend_lbt where user_id='" + user_id+"' and subject_id='"+subject_id + "'",connection1);
            //get competency_list of all user in subject
            ResultSet rs_all_user_sub = Proficiency.getResultSet_Execute("select progress->'competency_list' from deepend_lbt where subject_id='"+subject_id + "'",connection1);


            // whole calculation will remain same.

            JSONObject obj=null;
            JSONArray C = null;
            while(rs_user_sub.next()){
                //get list of all code and badge in jsonArray C
                C=new JSONArray(rs_user_sub.getString(1));
            }
            //initialise empty array lscores to store all percentile scores earned by user_id
            ArrayList lscores=new ArrayList();
            JSONObject proficiency=new JSONObject();
            for (ArrayList<String> key : grade_domain_mapping.keySet()) {
                lscores=Proficiency.get_lscores(C,rs_all_user_sub,user_id,user_badge,connection1, grade_domain_mapping.get(key));
                //reinitailse resultset to first row and get badge of user
                rs_all_user_sub.beforeFirst();
                String s = key.get(0) + "_" + key.get(1);
                if(!lscores.isEmpty()) {
                    badge = Proficiency.getBadge(lscores, rs_all_user_sub);
                }
                else{
                    //System.out.println(s +  " " + "badge is null");
                    proficiency.put(s, "NA");
                    continue;
                }
                //String s = key.get(0) + "_" + key.get(1);
                proficiency.put(s, badge.get("badge"));
            }
            set_proficiency(user_id,subject_id,proficiency);
            System.out.println(proficiency);
            // loop ends here.
        }
        connection1.close();
    }
}
