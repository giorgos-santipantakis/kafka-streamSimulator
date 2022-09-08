package main;

import java.text.DecimalFormat;

public class progressBar {

	private static DecimalFormat df = new DecimalFormat("#.###");
	public static final String TEXT_GREEN = "\u001B[32m";
	public static final String TEXT_RED = "\u001B[31m";
	public static final String TEXT_BLACK = "\u001B[30m";
	public static final String TEXT_RESET = "\u001B[0m";
	public static final String TEXT_ERASE_TO_END_OF_LINE = "\u001B[K";
	private static final String bar = "━";
	//used in progress function
	private long t0 = 0;
	private long lastCheck = 0;
	private String time_left = "";
	
	private String status = "";
	
	public void reset() {
		t0 = 0;
		lastCheck = 0;
		time_left = "";
		status = "";
	}
	
	public void progress(long current, long total) {
		double v = (double)current/(double)total;
		long t = System.currentTimeMillis();
		if(t0==0)t0=t;
		if(lastCheck==0)lastCheck=t;
		if(t-lastCheck>10000) {
			try {
				long t_left = (long)((1-v)*(t-t0)/v);
				time_left = millis2human(t_left);
				lastCheck=t;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		status = df.format(v*100d) + "% " + "ESTIMATED TIME LEFT: " + time_left;
		System.out.print(status + "          \r");		
	}
	public String getStatus() {return status;}

	public void progress(String prefix, long current, long total) {
		double v = (double)current/(double)total;
		long t = System.currentTimeMillis();
		if(t0==0)t0=t;
		if(lastCheck==0)lastCheck=t;
		if(t-lastCheck>10000) {
			try {
				long t_left = (long)((1-v)*(t-t0)/v);
				time_left = millis2human(t_left);
				lastCheck=t;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.print(prefix + " " + df.format(v*100d) + "% " + "ESTIMATED TIME LEFT: " + time_left + "          \r");		
	}
	
	public void progressGUI(String prefix, long current, long total) {
		double v = (double)current/(double)total;
		long t = System.currentTimeMillis();
		if(t0==0)t0=t;
		if(lastCheck==0)lastCheck=t;
		if(t-lastCheck>10000) {
			try {
				long t_left = (long)((1-v)*(t-t0)/v);
				time_left = millis2human(t_left);
				lastCheck=t;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		int g = (int)Math.round(v*10);
		String gui = makeBar(g, TEXT_GREEN) + makeBar(10-g, TEXT_BLACK);
		
		System.out.print(prefix + " " + gui + " " + df.format(v*100d) + "% " + "ESTIMATED TIME LEFT: " + time_left + "          \r");		
	}
	
	private String makeBar(int g, String color) {
		String r = color;
		for(int i=0;i<g;i++)r = r + bar;
		return r + TEXT_RESET;
	}

	public static String millis2human(long t_) {
		if(t_==0) return "";
		long[] threshold = new long[] {3600000l,60000l,1000l};
		String[] unit = new String[] {"hours","minutes","seconds"};
		
		for(int i=0;i<threshold.length;i++) {
			if(t_>threshold[i]) {
				long x = (t_)/(threshold[i]);
				long l = (t_)%(threshold[i]);
				return df.format(x) + " " + unit[i] + " " + millis2human(l);
			}
		}
		return String.valueOf(t_) + " msec";
	}


    public void showMessage(String s) {
		System.out.println(s+TEXT_ERASE_TO_END_OF_LINE);
    }

	private final String[] hourglass = new String[]{"-", "\\" , "|" , "/" };
    public void hourglass(String s, int i, boolean useANSI) {
		if(useANSI) {
			int pos = i % 10; //make sure that i is in range [0,9]
			System.out.print(s + " " + hourglass(pos + 1) + TEXT_ERASE_TO_END_OF_LINE + "\r");
		}else{
			System.out.print(s + " " + hourglass[i % 4] + "       \r");
		}
    }

	private String hourglass(int p){
		String pre = makeBar(p-1,TEXT_BLACK);
		String c = makeBar(1,TEXT_GREEN);
		String after = makeBar(10-p,TEXT_BLACK);
		return pre + c + after;
	}

	public void progressGUI(int current, long total, String suffix) {
		double v = (double)current/(double)total;
		long t = System.currentTimeMillis();
		if(t0==0)t0=t;
		if(lastCheck==0)lastCheck=t;
		if(t-lastCheck>10000) {
			try {
				long t_left = (long)((1-v)*(t-t0)/v);
				time_left = millis2human(t_left);
				lastCheck=t;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		int g = (int)Math.round(v*10);
		String gui = makeBar(g, TEXT_GREEN) + makeBar(10-g, TEXT_BLACK);
		
		System.out.print(" " + gui + " " + df.format(v*100d) + "% " + "ESTIMATED TIME LEFT: " + time_left + " " + suffix + TEXT_ERASE_TO_END_OF_LINE + "\r");
	}
}
