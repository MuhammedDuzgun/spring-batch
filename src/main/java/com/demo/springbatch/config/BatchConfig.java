package com.demo.springbatch.config;

import com.demo.springbatch.entity.Student;
import com.demo.springbatch.repository.StudentRepository;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

@Configuration
public class BatchConfig {

    private final StudentRepository studentRepository;

    public BatchConfig(StudentRepository studentRepository) {
        this.studentRepository = studentRepository;
    }

    @Bean
    public FlatFileItemReader<Student> itemReader() {
        FlatFileItemReader<Student> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("src/main/resources/students.csv"));
        itemReader.setName("CSV-Reader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(linemapper());
        return itemReader;
    }

    @Bean
    public StudentProcessor studentProcessor() {
        return new StudentProcessor();
    }

    @Bean
    public RepositoryItemWriter<Student> itemWriter(StudentRepository studentRepository) {
        RepositoryItemWriter<Student> itemWriter = new RepositoryItemWriter<>();
        itemWriter.setRepository(studentRepository);
        itemWriter.setMethodName("save");
        return itemWriter;
    }

    private LineMapper<Student> linemapper() {
        DefaultLineMapper<Student> lineMapper = new DefaultLineMapper<>();

        // Define the line tokenizer to parse the CSV file
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "name", "lastName", "age");

        // Set the tokenizer to the line mapper
        BeanWrapperFieldSetMapper<Student> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Student.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }


}
