package hdfs;
import java.io.Serializable;

public class CommandeEtDonnees implements Serializable {
	String commande;
	byte[] donnees;
	
	public CommandeEtDonnees(String c, byte[] data)
	{
		this.commande = c;
		this.donnees = data;
	}
}
