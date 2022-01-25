# Projet_BDD

## README:

### Project tree:
  ![tree](https://user-images.githubusercontent.com/57391709/150883453-35de59ae-3fb4-45e6-a607-ca21e0cb37d0.JPG)  
  
### Installation:

Here is the **root** of our project, to launch the project and be able to launch everything else you have to type the following command:

- If you have never started the project:

> docker-compose up --build

- If you have already started the project:

> docker-compose up -d

After installing all packages and launching all images, you should have 6 images running on your **docker container**.

### Run project:

To launch the project you will have to run 2 scripts. 1 on the root of the project and 1 on the docker image of pyspark_notebook.
- Connect to the docker image of **pyspark_notebook** on a terminal at the root of the project:

> docker exec -it pyspark_notebook bash

- Enter :

> cd ../work

#### Consumer part:

You have the choice to launch the consumers of our project in 3 different ways. **Start**, **Restart**, **Reset**.
- With **Start** you will launch the project under normal circumstances:

> ./start.sh

- With **Restart** you will restart the project when there is an error or problem:

> ./restart.sh

- With **Reset** you will start the project by deleting all the data saved so far:

> ./reset.sh

#### Producer part:

Get to the **root** of the project.

- Run the producer script:

> .\start_producer.sh

