from datetime import datetime
from elasticsearch_dsl import DocType, Date, Integer, Keyword, Text, connections

# Define a default Elasticsearch client
connections.create_connection(hosts=['119.29.223.117'])

class Article(DocType):
    title = Text(analyzer='snowball', fields={'raw': Keyword()})
    body = Text(analyzer='snowball')
    tags = Keyword()
    published_from = Date()
    lines = Integer()

    class Meta:
        index = 'blog'

    def save(self, ** kwargs):
        self.lines = len(self.body.split())
        return super(Article, self).save(** kwargs)

    def delete(self, ** kwargs):
        self.lines = len(self.body.split())
        return super(Article, self).delete(** kwargs)

    def is_published(self):
        return datetime.now() > self.published_from

# create the mappings in elasticsearch


# create and save and article
article = Article(meta={'id': 41}, title='Hello world!', tags=['test'])
article.body = ''' looong text '''
article.published_from = datetime.now()
article.save()

article = Article.get(id=42)
print(article.is_published())
article

# Display cluster health
print(connections.get_connection().cluster.health())