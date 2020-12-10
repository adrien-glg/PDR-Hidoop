/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import configuration.Parametres; 

public class HdfsClient {

    private Socket[] sockets;

    /*Constructeur qui permet de définir une liste de sockets en fonction du fichier configuration défini*/ 
    public HdfsClient(){
        /*Nombre de sockets à définir*/ 
        int n = Parametres.adresses.length;
        sockets = new Socket[n]; 
        /*Boucle initialisant chaque socket et réalisant une démande de connexion*/
        for (int i = 0; i<n; i++){
            sockets[i] = new Socket(Parametres.adresses[i],Parametres.ports[i]);
            println("Connecté à la machine" + Parametres.adresses[i] + " sur le port " + Parametres.ports[i]);
        }
    }

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }
	
    public static void HdfsDelete(String hdfsFname) {}
	
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, 
     int repFactor) { }

    public static void HdfsRead(String hdfsFname, String localFSDestFname) {
        /*Déclaraiton des OutputStream qui serviront pour envoyer la commande aux nodes*/ 
        
        /*Definition du message qui va être envoyé par le socket*/

        /**/  

     }

	
    public static void main(String[] args) {
        // java HdfsClient <read|write> <line|kv> <file>

        
        try {
            if (args.length<2) {usage(); return;}
            /*Initialisation des sockets selon le fichier configuration*/
            this = new HdfsClient(); 

            switch (args[0]) {
              case "read": HdfsRead(args[1],Parametres.path + args[1]); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write": 
                Format.Type fmt;
                if (args.length<3) {usage(); return;}
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {usage(); return;}
                HdfsWrite(fmt,args[2],1);
            }	
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
