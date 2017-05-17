import evospace
import cherrypy

from cherrypy._cpcompat import ntou

import json, os
import simplejson

class Content:
    def __init__(self, popName="pop"):
        self.population = evospace.Population(popName)
        self.population.initialize()

    @cherrypy.expose
    #@cherrypy.tools.json_out()
    @cherrypy.tools.json_in(content_type=[ntou('application/json'),
                                          ntou('text/javascript'),
                                          ntou('application/json-rpc')
                                          ])
    def index(self):
        if cherrypy.request.json:
            obj = cherrypy.request.json
            method = obj["method"]
            _id = obj["id"]
            #result=obj

            if "params" in obj:
                params = obj["params"]
            # else:
            #     return json.dumps({"result": None, "error":
            #         {"code": -32604, "message": "Params empty"}, "id": _id})
            #
            # # process the data
            cherrypy.response.headers['Content-Type'] = 'text/json-comment-filtered'
            result = None
            if method == "initialize":
                result = self.population.initialize()
                return simplejson.dumps({"result": result, "error": None, "id": _id})
            #
            if method == "getSample":
                result = self.population.get_sample(params[0])
                if result:
                    return json.dumps({"result": result, "error": None, "id": _id})
                else:
                    return json.dumps({"result": None, "error":
                        {"code": -32601, "message": "EvoSpace empty"}, "id": _id})
            elif method == "respawn":
                result = self.population.respawn(params[0])
            elif method == "putZample":
                result = self.population.put_sample_specie(params[0])
            elif method == "putSpecie":
                result = self.population.put_specieinfo(params[0])
            elif method == "getIntraSpecie":
                result = self.population.get_speciedistance(params[0])
            elif method == "getSpecieInfo":
                result = self.population.get_specieinfo(params[0])
            elif method == "putIndividual":
                result = self.population.put_individual(**params[0])
            elif method == "size":
                result = self.population.size()
            elif method == "found":
                result = self.population.found()
            elif method == "found_it":
                result = self.population.found_it()
            elif method == "get_CounterSpecie":
                result = self.population.get_at_specie()
            elif method == "getSampleNumber":
                result = self.population.get_returned_counter()
            elif method == "getPopulation":
                result = self.population.get_population()
                if result:
                    return json.dumps({"result": result, "error": None, "id": _id})
                else:
                    return json.dumps({"result": None, "error":
                        {"code": -32601, "message": "EvoSpace empty"}, "id": _id})
            elif method == "getReadAll":
                result = self.population.read_all()
            elif method == "getSpecie":
                result = self.population.get_species()
            elif method == "getSample_specie":
                result = self.population.get_sample_specie(params[0])
                if result:
                    return json.dumps({"result": result, "error": None, "id": _id})
                else:
                    return json.dumps({"result": None, "error":
                        {"code": -32601, "message": "EvoSpace Specie empty"}, "id": _id})

            return simplejson.dumps({"result": result, "error": None, "id": _id})

        else:
            return simplejson.dumps({"result": 'HOLA', "error": None, "id": [0.]})


    # @cherrypy.expose
    # @cherrypy.tools.json_out()
    # def index(self):
    #     return json.dumps({"result": 'Mundo', "error": None, "id": [0.]})


if __name__ == '__main__':
    cherrypy.config.update({'server.socket_host': '0.0.0.0',
                            'server.socket_port': int(os.environ.get('PORT', '5000'))
                               , 'server.environment': 'production'
                               , 'server.thread_pool': 200
                               , 'tools.sessions.on': False
                               , 'server.socket_timeout': 30
                            })

    from cherrypy.process import servers


    def fake_wait_for_occupied_port(host, port):
        return


    servers.wait_for_occupied_port = fake_wait_for_occupied_port

    cherrypy.quickstart(Content('pop'))