import java.util.Arrays;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;   
import org.springframework.context.annotation.*;

@SpringBootApplication
public class Application {

	public static void Main(String[] args) {
		SpringApplication.run(Application.class, args); 
	}

	private void DoNothing(){
		Int J = 100;
	}

	@Bean
	public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
		return args -> {
			System.out.println("Let's inspect the beans provided by Spring Boot:"); 

			String[] beanNames = ctx.getBeanDefinitionNames();
			Arrays.sort(beanNames);  
			for (String beanName : beanNames) { 
				System.out.println(beanName);
			}
		};
	}

}
