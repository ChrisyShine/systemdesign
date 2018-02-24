package chrisyshine.systemdesign.twitter;

import java.util.List;

import chrisyshine.systemdesign.twitter.dto.Feed;
import chrisyshine.systemdesign.twitter.service.Service;

/**
 * Hello world!
 *
 */
public class App {
	Service service;
	public App() {
		service = new Service();
	}

	
    public static void main(String[] args) {
        System.out.println("Hello World!");
        App app = new App();
        
//        System.out.println("0001 post a feed 'Hello World!'");
//        app.service.postFeed("0001", "Hello World!");
//        System.out.println("0001 get news feed");
//        printFeeds(app.service.getNewsFeed("0001"));
//        System.out.println("0001 follows 0002");
//        app.service.follow("0001", "0002");
//        System.out.println("0002 post a feed 'How are you today?'");
//        app.service.postFeed("0002", "How are you today?");
        
        
//        System.out.println("0001 unfollows 0002");
//        app.service.unFollow("0001", "0002");
//        System.out.println("0001 get news feed");
//        printFeeds(app.service.getNewsFeed("0001"));
//        System.out.println("0001 follows 0002");
//        app.service.follow("0001", "0002");
        System.out.println("0001 get news feed");
        printFeeds(app.service.getNewsFeed("0001"));
        app.service.close();
    }
    
    public static void printFeeds(List<Feed> feeds) {
    	feeds.stream().forEach(feed -> System.out.println(feed.stringify()));
    }
    
}
