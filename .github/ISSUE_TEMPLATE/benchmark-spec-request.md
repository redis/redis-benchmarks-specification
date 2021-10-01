name: Benchmark Specification Request

description: File a benchmark SPEC request for improvement or addition

title: "[SPEC]: "

labels: ["spec", "triage"]

assignees:
  - filipecosta90

body:
  - type: markdown
    attributes:
      value: |
        Thanks for taking the time to fill out this benchmark specification request for improvement or addition!
        Here's our manifesto:
    
        - The Redis benchmarks specification describes the cross-language/tools requirements and expectations to foster 
        performance and observability OPEN standards around Redis related technologies.

        - Members from both industry and academia, including organizations and individuals are encouraged to contribute. 
        
        - This Project is Provider/Company/Org independent and fosters true openness about Performance, meaning the SOLE
        purpose of it is to improve/retain Redis Performance.
    
        Make sure to double check if this this benchmark specification request for improvement or addition hasn't 
        already been asked for at the [issues section](https://github.com/redis/redis-benchmarks-specification/labels/spec).
        

  - type: dropdown
    id: type
    attributes:
      label: Improvement/Addition type
      description: What changes to the current SPEC does this issue focuses upon?
      options:
        - Add a benchmark variant
        - Improve a benchmark variant
        - Raise awareness for a new Redis benchmark tool
        - Suggest a different Redis Build Variant
        - Other (fill details in section bellow)
    
  - type: textarea
    id: details
    attributes:
      label: Detail the requested changes
      description: Include reference to the benchmarks/tools you've seen and want to see part of the spec.
      placeholder: Include reference to the benchmarks/tools you've seen and want to see part of the spec.
    validations:
      required: true
    