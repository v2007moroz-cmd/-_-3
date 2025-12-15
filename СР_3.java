public class DataPipelineApp 
{
    private static final Logger log = LoggerFactory.getLogger(DataPipelineApp.class);

    public static void main(String[] args) 
    {
  
        String inputFile = args.length > 0 ? args[0] : "input.txt";
        String url = args.length > 1 ? args[1] : "https://example.com";
        String outputZip = args.length > 2 ? args[2] : "out/report.zip";
        String jarPath = args.length > 3 ? args[3] : ""; 

        log.info("=== DataPipelineApp started ===");
        log.info("Input file: {}", inputFile);
        log.info("URL: {}", url);
        log.info("Output ZIP: {}", outputZip);
        if (!jarPath.isBlank()) log.info("JAR to inspect: {}", jarPath);

        StringBuilder report = new StringBuilder();
        report.append("REPORT @ ").append(Instant.now()).append("\n\n");

        try 
        {
            report.append("[FILE]\n");
            List<String> lines = FileModule.readAllLines(inputFile);
            report.append("Read lines: ").append(lines.size()).append("\n");
            report.append("Sample: ").append(lines.stream().findFirst().orElse("(empty)")).append("\n\n");
        } catch (FileProcessingException e) 
        {
            log.error("File processing failed: {}", e.getMessage(), e);
            report.append("File error: ").append(e.getMessage()).append("\n\n");
        } finally {

            log.info("File stage finished (finally executed).");
        }

        try 
        {
            report.append("[NETWORK]\n");
            String payload = NetworkModule.fetchText(url, 20_000);
            report.append("Downloaded chars: ").append(payload.length()).append("\n");
            report.append("First 120 chars: ").append(payload.substring(0, Math.min(120, payload.length()))).append("\n\n");
        } catch (NetworkProcessingException e) 
        {
            log.warn("Network failed: {}", e.getMessage());
            report.append("Network error: ").append(e.getMessage()).append("\n\n");
        }

       try 
       {
            report.append("[DB]\n");
            DbModule.Result dbRes = DbModule.runDemo();
            report.append("Inserted rows: ").append(dbRes.inserted).append("\n");
            report.append("Selected rows: ").append(dbRes.selected).append("\n");
            report.append("Sample row: ").append(dbRes.sampleRow).append("\n\n");
        } catch (DbProcessingException e) 
        {
            log.error("DB failed: {}", e.getMessage(), e);
            report.append("DB error: ").append(e.getMessage()).append("\n\n");
        }

      
        try {
            report.append("[SERIALIZATION]\n");

            UserProfile serializableObj = new UserProfile("olena", 30, List.of("java", "sql", "oop"));
            AuditRecord externalizableObj = new AuditRecord("LOGIN", "olena", Instant.now().toString());

            Path outDir = Paths.get("out");
            Files.createDirectories(outDir);

            Path serPath = outDir.resolve("userprofile.ser");
            Path extPath = outDir.resolve("audit.ext");

            SerializationModule.saveSerializable(serPath, serializableObj);
            UserProfile loaded = SerializationModule.loadSerializable(serPath, UserProfile.class);

            SerializationModule.saveExternalizable(extPath, externalizableObj);
            AuditRecord loadedAudit = SerializationModule.loadExternalizable(extPath, AuditRecord::new);

            report.append("Serializable saved+loaded: ").append(loaded).append("\n");
            report.append("Externalizable saved+loaded: ").append(loadedAudit).append("\n\n");

        } catch (IOException | ClassNotFoundException e) 
        {
            log.error("Serialization stage failed: {}", e.getMessage(), e);
            report.append("Serialization error: ").append(e.getMessage()).append("\n\n");
        }

        try {
            report.append("[ZIP]\n");
            Path zipPath = Paths.get(outputZip);
            ZipJarModule.writeReportToZip(zipPath, "report.txt", report.toString());

            List<String> entries = ZipJarModule.listZipEntries(zipPath);
            report.append("ZIP created. Entries: ").append(entries).append("\n\n");
            log.info("ZIP created at {}", zipPath.toAbsolutePath());

        } catch (IOException e) {
            log.error("ZIP stage failed: {}", e.getMessage(), e);
        }

 
        if (!jarPath.isBlank()) 
        {
            try 
            {
                report.append("[JAR]\n");
                List<String> jarEntries = ZipJarModule.listJarEntries(Paths.get(jarPath));
                report.append("JAR entries count: ").append(jarEntries.size()).append("\n");
                report.append("First 20 entries:\n");
                jarEntries.stream().limit(20).forEach(s -> report.append(" - ").append(s).append("\n"));
                report.append("\n");
            } catch (IOException e) 
            {
                log.warn("JAR inspect failed: {}", e.getMessage());
                report.append("JAR error: ").append(e.getMessage()).append("\n\n");
            }
        }

   
        try 
        {
            Files.createDirectories(Paths.get("out"));
            Files.writeString(Paths.get("out", "report.txt"), report.toString(), StandardCharsets.UTF_8);
            log.info("Plain report saved: out/report.txt");
        }
         catch (IOException e) 
         {
            log.warn("Could not save plain report: {}", e.getMessage());
        }

        log.info("=== DataPipelineApp finished ===");
    }

    static class FileProcessingException extends Exception 
    {
        public FileProcessingException(String message, Throwable cause) { super(message, cause); }
        public FileProcessingException(String message) { super(message); }
    }

    static class NetworkProcessingException extends Exception 
    {
        public NetworkProcessingException(String message, Throwable cause) { super(message, cause); }
        public NetworkProcessingException(String message) { super(message); }
    }

    static class DbProcessingException extends Exception 
    {
        public DbProcessingException(String message, Throwable cause) { super(message, cause); }
        public DbProcessingException(String message) { super(message); }
    }

    static class FileModule 
    {

        static List<String> readAllLines(String path) throws FileProcessingException 
        {
            Path p = Paths.get(path);

            if (!Files.exists(p)) 
            {
 
                throw new FileProcessingException("Input file not found: " + p.toAbsolutePath());
            }

            try (BufferedReader br = Files.newBufferedReader(p, StandardCharsets.UTF_8)) 
            {
                List<String> lines = new ArrayList<>();
                String line;
                while ((line = br.readLine()) != null) lines.add(line);
                log.info("Read {} lines from file", lines.size());
                return lines;
            } catch (IOException e) 
            {
                throw new FileProcessingException("Failed to read file: " + p.toAbsolutePath(), e);
            }
        }
    }

  
    static class NetworkModule 
    {
        static String fetchText(String url, int timeoutMs) throws NetworkProcessingException {
            HttpURLConnection conn = null;
            try 
            {
                URL u = new URL(url);
                conn = (HttpURLConnection) u.openConnection();
                conn.setConnectTimeout(timeoutMs);
                conn.setReadTimeout(timeoutMs);
                conn.setRequestMethod("GET");

                int status = conn.getResponseCode();
                log.info("HTTP status: {}", status);

                InputStream is = (status >= 200 && status < 300) ? conn.getInputStream() : conn.getErrorStream();
                if (is == null) throw new IOException("No response stream");


                try (InputStream in = is;
                     ByteArrayOutputStream baos = new ByteArrayOutputStream()) 
                     {
                    in.transferTo(baos);
                    return baos.toString(StandardCharsets.UTF_8);
                }
            } 
            catch (IOException e)
           {
                throw new NetworkProcessingException("Network fetch failed for: " + url, e);
            } finally {
                if (conn != null) conn.disconnect();
            }
        }
    }

    static class DbModule 
    {
        static class Result 
        {
            final int inserted;
            final int selected;
            final String sampleRow;
            Result(int inserted, int selected, String sampleRow) 
            {
                this.inserted = inserted;
                this.selected = selected;
                this.sampleRow = sampleRow;
            }
        }

        static Result runDemo() throws DbProcessingException 
        {
            String jdbcUrl = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
            String user = "sa";
            String pass = "";

            try (Connection conn = DriverManager.getConnection(jdbcUrl, user, pass)) 
            {
                conn.setAutoCommit(false);

                try (Statement st = conn.createStatement()) 
                {
                    st.execute("""
                        CREATE TABLE IF NOT EXISTS events(
                            id IDENTITY PRIMARY KEY,
                            type VARCHAR(50),
                            username VARCHAR(50),
                            created_at VARCHAR(50));  
                         """);
                }

                int inserted;
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO events(type, username, created_at) VALUES(?,?,?)")) 
                {
                    inserted = 0;
                    inserted += insertEvent(ps, "LOGIN", "olena");
                    inserted += insertEvent(ps, "PAYMENT", "olena");
                    inserted += insertEvent(ps, "LOGOUT", "olena");
                }

                int selected = 0;
                String sample = "(none)";
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT type, username, created_at FROM events ORDER BY id ASC")) 
                {
                    try (ResultSet rs = ps.executeQuery()) 
                    {
                        while (rs.next()) {
                            selected++;
                            if (selected == 1) {
                                sample = rs.getString(1) + ", " + rs.getString(2) + ", " + rs.getString(3);
                            }
                        }
                    }
                }

                conn.commit();
                log.info("DB demo ok: inserted={}, selected={}", inserted, selected);
                return new Result(inserted, selected, sample);

            } catch (SQLException e) 
            {
                throw new DbProcessingException("DB operation failed (H2).", e);
            }
        }

        private static int insertEvent(PreparedStatement ps, String type, String username) throws SQLException {
            ps.setString(1, type);
            ps.setString(2, username);
            ps.setString(3, Instant.now().toString());
            return ps.executeUpdate();
        }
    }


    static class SerializationModule {

        static void saveSerializable(Path path, Serializable obj) throws IOException {
            try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(path))) 
            {
                oos.writeObject(obj);
                log.info("Serializable saved: {}", path.toAbsolutePath());
            }
        }

        static <T> T loadSerializable(Path path, Class<T> cls) throws IOException, ClassNotFoundException 
        {
            try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(path))) 
            {
                Object obj = ois.readObject();
                return cls.cast(obj);
            }
        }

        static void saveExternalizable(Path path, Externalizable obj) throws IOException {
            try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(path))) 
            {
                oos.writeObject(obj);
                log.info("Externalizable saved: {}", path.toAbsolutePath());
            }
        }

        interface Factory<T> { T create(); }

        static <T extends Externalizable> T loadExternalizable(Path path, Factory<T> factory)
                throws IOException, ClassNotFoundException {
            try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(path))) 
            {
                Object obj = ois.readObject();
                @SuppressWarnings("unchecked")
                T casted = (T) obj;
                return casted;
            }
        }
    }


    static class UserProfile implements Serializable 
    {
        private static final long serialVersionUID = 1L;

        private String username;
        private int age;
        private List<String> tags;

        public UserProfile(String username, int age, List<String> tags) 
        {
            this.username = username;
            this.age = age;
            this.tags = new ArrayList<>(tags);
        }

        @Override
        public String toString() {
            return "UserProfile{username='" + username + "', age=" + age + ", tags=" + tags + "}";
        }
    }

    static class AuditRecord implements Externalizable 
    {
        private String type;
        private String username;
        private String createdAt;

        public AuditRecord() {}

        public AuditRecord(String type, String username, String createdAt) 
        {
            this.type = type;
            this.username = username;
            this.createdAt = createdAt;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException 
        {
            out.writeUTF(type != null ? type : "");
            out.writeUTF(username != null ? username : "");
            out.writeUTF(createdAt != null ? createdAt : "");
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException 
        {
            this.type = in.readUTF();
            this.username = in.readUTF();
            this.createdAt = in.readUTF();
        }

        @Override
        public String toString() 
        {
            return "AuditRecord{type='" + type + "', username='" + username + "', createdAt='" + createdAt + "'}";
        }
    }

   
    static class ZipJarModule 
    {

        static void writeReportToZip(Path zipPath, String entryName, String content) throws IOException {
            Files.createDirectories(zipPath.getParent() != null ? zipPath.getParent() : Paths.get("."));

            try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zipPath))) {
                ZipEntry entry = new ZipEntry(entryName);
                zos.putNextEntry(entry);
                byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
                zos.write(bytes);
                zos.closeEntry();
            }
        }

        static List<String> listZipEntries(Path zipPath) throws IOException 
        {
            List<String> entries = new ArrayList<>();
            try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zipPath))) 
            {
                ZipEntry e;
                while ((e = zis.getNextEntry()) != null) 
                {
                    entries.add(e.getName());
                    zis.closeEntry();
                }
            }
            return entries;
        }

        static List<String> listJarEntries(Path jarPath) throws IOException 
        {
            try (JarFile jar = new JarFile(jarPath.toFile())) 
            {
                List<String> entries = new ArrayList<>();
                jar.stream().forEach(je -> entries.add(je.getName()));
                return entries;
            }
        }
    }
}
