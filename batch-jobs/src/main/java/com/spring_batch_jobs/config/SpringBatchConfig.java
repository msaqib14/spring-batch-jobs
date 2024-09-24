package com.spring_batch_jobs.config;

import com.spring_batch_jobs.entity.Customer;
import com.spring_batch_jobs.repository.CustomerRepository;
import net.bytebuddy.implementation.bind.annotation.Super;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class SpringBatchConfig {
    // Add batch job configuration here (e.g., job parameters, reader, writer, processor)
    @Autowired
    private  final JobBuilderFactory jobBuilderFactory;

    @Autowired
    private final StepBuilderFactory stepBuilderFactory;

    @Autowired
    private final CustomerRepository customerRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    public SpringBatchConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, CustomerRepository customerRepository){
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.customerRepository = customerRepository;
    }

    @Value("${file.location}")
    private String fileLocation;

    @Bean
    FlatFileItemReader<Customer> reader(){
        final FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource(fileLocation));
        itemReader.setName("txtFileReader");
        itemReader.setLinesToSkip(1); // Skip the header row
        itemReader.setStrict(false);
        itemReader.setLineMapper(lineMapper());
        return itemReader;
    }


    private LineMapper<Customer> lineMapper() {
        final DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();
        final DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter("|");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "first_name", "last_Name", "email", "gender", "contact", "country");

        //Mapping with java object (Customer object)
        final BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;

    }

    @Bean
    RepositoryItemWriter<Customer> deleteItemWriter(){
        final RepositoryItemWriter<Customer> itemWriter = new RepositoryItemWriter<>();
        itemWriter.setRepository(customerRepository);
        itemWriter.setMethodName("delete");
        return itemWriter;
    }
    @Bean
    RepositoryItemWriter<Customer> insertItemWriter(){
        final RepositoryItemWriter<Customer> itemWriter = new RepositoryItemWriter<>();
        itemWriter.setRepository(customerRepository);
        itemWriter.setMethodName("save");
        return itemWriter;
    }

    @Bean
    CompositeItemWriter<Customer>  compositeItemWriter(){
        final CompositeItemWriter<Customer> compositeItemWriter = new CompositeItemWriter<>();
        final List<ItemWriter<? super Customer>> items = new ArrayList<>();
        items.add(deleteItemWriter());
        items.add(insertItemWriter());
        compositeItemWriter.setDelegates(items);
        return compositeItemWriter;


    }

    @Bean
    Step loadCustomerDataStep(){
        return stepBuilderFactory.get("dat-step").<Customer,Customer>chunk(10).reader(reader())
                .writer(compositeItemWriter()).taskExecutor(taskExecutor()).transactionManager(transactionManager).build();

    }

    @Bean
    Job runJob(){
        return jobBuilderFactory.get("load-customer-data-job").flow(loadCustomerDataStep()).end().build();
    }

    @Bean
    TaskExecutor taskExecutor(){
    final SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
    asyncTaskExecutor.setConcurrencyLimit(10);
    return asyncTaskExecutor;

    }
    


}
