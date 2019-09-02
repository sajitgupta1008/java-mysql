public class Application {
    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.out.println("please provide the following arguments in the given order - host, username, password");
        }

        new MySQLAccess().readDataBase(args[0].trim(), args[1].trim(), args[2].trim());
    }
}
