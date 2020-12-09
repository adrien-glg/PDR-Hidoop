package ordo;

import java.util.concurrent.Semaphore;

public class CallBackImpl implements CallBack {
	
	private int compteur;
	private int nbFrag;
	private Semaphore sem;
	
	public CallBackImpl(int nbF, Semaphore s) {
		this.compteur = 0;
		this.nbFrag = nbF;
		this.sem = s;
	}
	
	public void incr() {
		this.compteur++;
		sem.release();
	}
	
	public boolean isFinished() {
		return this.compteur == this.nbFrag;
	}
	
}
