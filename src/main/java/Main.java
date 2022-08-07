import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.Document;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class Main
{
    private static final String path = "src/main/resources/mongo.csv";
    public static void main(String[] args) throws IOException {

        List<String> lines = Files.readAllLines(Paths.get(path));
        List<String> students = new ArrayList<>();
        for (String s : lines) {
            StringBuilder builder = new StringBuilder(s);
            boolean inQuotes = false;

            for (int currentIndex = 0; currentIndex < builder.length(); currentIndex++) {
                char currentChar = builder.charAt(currentIndex);
                if (currentChar == '\"') {
                    inQuotes = !inQuotes;
                }
                if (currentChar == ',' && inQuotes) {
                    builder.setCharAt(currentIndex, '/');
                }
            }
            String line = builder.toString();
            students.add(line);
        }


        MongoClient mongoClient = new MongoClient("127.0.0.1" , 27017);
        MongoDatabase database = mongoClient.getDatabase("students");
        MongoCollection<Document> collection = database.getCollection("students");
        collection.drop();

        students.forEach(s -> {
            String[] data = s.split(",");
            Document coursesObject = new Document();
            String[] courses = data[2].replace("\"", "").replace("/", ", ").split(",");
            List<String> coursesList = new ArrayList<>();
            coursesList.addAll(Arrays.asList(courses));

            coursesObject.put("courses", coursesList);

            Document document = new Document()
                    .append("name", data[0])
                    .append("age", Integer.parseInt(data[1]))
                    .append("courses", coursesObject);
            collection.insertOne(document);
        });


        System.out.println("Total students:  " + collection.countDocuments());

        BsonDocument query = BsonDocument.parse("{age: {$gt: 40}}");
        System.out.println("\nStudents over 40: " + collection.countDocuments(query));

        System.out.println("\nList of the youngest students: ");

        query = BsonDocument.parse("{age: {$lte: 18}}");
        collection.find(query)
                .forEach((Consumer<Document>) document -> System.out.println("\n" + document.get("name") + ", " + document.get("age")));

        System.out.println("\nList of the oldest students: ");

        query = BsonDocument.parse("{age: {$gt: 40}}");
        collection.find(query)
                .forEach((Consumer<Document>) d -> {
                    Document document = (Document) d.get("courses");
                    System.out.println("\n" + d.get("name") + ", courses: " + document.get("courses"));
                });
    }
}