package ordo;

public class CallBackImpl implements CallBack {
	
	private int compteur;
	private int nbFrag;
	
	public CallBackImpl(int nbF) {
		this.compteur = 0;
		this.nbFrag = nbF;
	}
	
	public void incr() {
		this.compteur++;
	}
	
	public boolean isFinished() {
		return this.compteur == this.nbFrag;
	}
	
}
