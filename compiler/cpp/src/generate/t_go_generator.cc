/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <string>
#include <fstream>
#include <iostream>
#include <vector>

#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sstream>
#include <algorithm>
#include "t_generator.h"
#include "platform.h"
#include "version.h"

using namespace std;

string identity(const string& name) { return name; }

/**
 * Go code generator.
 *
 */
class t_go_generator : public t_generator {
 public:
  t_go_generator(
      t_program* program,
      const std::map<std::string, std::string>& parsed_options,
      const std::string& option_string)
    : t_generator(program)
  {
    std::map<std::string, std::string>::const_iterator iter;
    out_dir_base_ = "gen-go";
  }

  /**
   * Init and close methods
   */

  void init_generator();
  void close_generator();

  /**
   * Program-level generation functions
   */

  void generate_typedef  (t_typedef*  ttypedef);
  void generate_enum     (t_enum*     tenum);
  void generate_const    (t_const*    tconst);
  void generate_struct   (t_struct*   tstruct);
  void generate_xception (t_struct*   txception);
  void generate_service  (t_service*  tservice);

  std::string render_const_value(t_type* type, t_const_value* value, const string& name);

  /**
   * Struct generation code
   */

  void generate_go_struct(t_struct* tstruct, bool is_exception);
  void generate_go_struct_definition(std::ofstream& out, t_struct* tstruct, bool is_xception=false, bool is_result=false);
  void generate_go_function_helpers(t_function* tfunction);

  /**
   * Service-level generation functions
   */

  void generate_service_helpers   (t_service*  tservice);
  void generate_service_interface (t_service* tservice);
  void generate_service_client    (t_service* tservice);
  void generate_service_remote    (t_service* tservice);
  void generate_service_server    (t_service* tservice);
  void generate_process_function  (t_service* tservice, t_function* tfunction);


  void generate_go_docstring         (std::ofstream& out,
                                          t_struct* tstruct);

  void generate_go_docstring         (std::ofstream& out,
                                          t_function* tfunction);

  void generate_go_docstring         (std::ofstream& out,
                                          t_doc*    tdoc,
                                          t_struct* tstruct,
                                          const char* subheader);

  void generate_go_docstring         (std::ofstream& out,
                                          t_doc* tdoc);

  /**
   * Helper rendering functions
   */

  std::string go_autogen_comment();
  std::string go_package();
  std::string go_imports(bool types);
  std::string render_includes(const std::set<string>& uses);
  std::string render_fastbinary_includes();
  std::string declare_argument(t_field* tfield);
  std::string render_field_default_value(t_field* tfield, const string& name);
  std::string type_name(t_type* ttype, 
			string (*filter)(const string&) = identity,
			const string& prefix = "");
  std::string function_signature(t_function* tfunction, std::string prefix="");
  std::string function_signature_if(t_function* tfunction, std::string prefix="", bool addOsError=false);
  std::string argument_list(t_struct* tstruct);
  std::string type_to_enum(t_type* ttype);
  std::string type_to_go_type(t_type* ttype, std::string ptr = "*");

  static std::string get_real_go_module(const t_program* program) {
    if (!program) return "";
    std::string real_module = program->get_namespace("go");
    if (real_module.empty()) {
      return program->get_name();
    }
    return real_module;
  }

 private:
  
  /**
   * File streams
   */

  std::set<string> includes_;
  std::ofstream f_types_;
  std::stringstream f_consts_;
  std::ofstream f_service_;

  std::string package_name_;
  std::string package_dir_;
  
  static std::string publicize(const std::string& value);
  static std::string privatize(const std::string& value);
  static std::string variable_name_to_go_name(const std::string& value);
  static bool can_be_nil(t_type* value);

  std::string go_type_prefix(t_type*);
  static std::set<string> get_go_includes(t_program*);
  static std::set<string> get_go_modules_used(t_service*, bool remote);
  static std::set<string> get_go_modules_used(const std::vector<t_typedef*>&);
  static std::set<string> get_go_modules_used(const std::vector<t_struct*>&);
  static std::set<string> get_go_modules_used(t_struct*);
  static string get_go_modules_used(t_type*);
};


std::string t_go_generator::publicize(const std::string& value) {
  if(value.size() <= 0) return value;
  std::string value2(value);
  if(!isupper(value2[0]))
    value2[0] = toupper(value2[0]);
  // as long as we are changing things, let's change _ followed by lowercase to capital
  for(string::size_type i=1; i<value2.size()-1; ++i) {
    if(value2[i] == '_' && isalpha(value2[i+1])) {
      value2.replace(i, 2, 1, toupper(value2[i+1]));
    }
  }
  return value2;
}

std::string t_go_generator::privatize(const std::string& value) {
  if(value.size() <= 0) return value;
  std::string value2(value);
  if(!islower(value2[0])) {
    value2[0] = tolower(value2[0]);
  }
  // as long as we are changing things, let's change _ followed by lowercase to capital
  for(string::size_type i=1; i<value2.size()-1; ++i) {
    if(value2[i] == '_' && isalpha(value2[i+1])) {
      value2.replace(i, 2, 1, toupper(value2[i+1]));
    }
  }
  return value2;
}

std::string t_go_generator::variable_name_to_go_name(const std::string& value) {
  if(value.size() <= 0) return value;
  std::string value2(value);
  std::transform(value2.begin(), value2.end(), value2.begin(), ::tolower);
  switch(value[0]) {
    case 'b':
    case 'B':
      if(value2 != "break") {
        return value;
      }
      break;
    case 'c':
    case 'C':
      if(value2 != "case" && value2 != "chan" && value2 != "const" && value2 != "continue") {
        return value;
      }
      break;
    case 'd':
    case 'D':
      if(value2 != "default" && value2 != "defer") {
        return value;
      }
      break;
    case 'e':
    case 'E':
      if(value2 != "else" && value2 != "error") {
        return value;
      }
      break;
    case 'f':
    case 'F':
      if(value2 != "fallthrough" && value2 != "for" && value2 != "func") {
        return value;
      }
      break;
    case 'g':
    case 'G':
      if(value2 != "go" && value2 != "goto") {
        return value;
      }
      break;
    case 'i':
    case 'I':
      if(value2 != "if" && value2 != "import" && value2 != "interface") {
        return value;
      }
      break;
    case 'm':
    case 'M':
      if(value2 != "map") {
        return value;
      }
      break;
    case 'p':
    case 'P':
      if(value2 != "package") {
        return value;
      }
      break;
    case 'r':
    case 'R':
      if(value2 != "range" && value2 != "return") {
        return value;
      }
      break;
    case 's':
    case 'S':
      if(value2 != "select" && value2 != "struct" && value2 != "switch") {
        return value;
      }
      break;
    case 't':
    case 'T':
      if(value2 != "type") {
        return value;
      }
      break;
    case 'v':
    case 'V':
      if(value2 != "var") {
        return value;
      }
      break;
    default:
      return value;
  }
  return value2 + "_";
}


std::set<string> t_go_generator::get_go_includes(t_program* program) {
  std::set<string> result;
  const vector<t_program*>& includes = program->get_includes();
  for (size_t i = 0; i < includes.size(); ++i) {
    result.insert(get_real_go_module(includes[i]));
  }
  return result;
  
}

static std::set<string>& modules_union(std::set<string>& modules, string module) {
  if (module.size() > 0) {
    modules.insert(module);
  }
  return modules;
}

static std::set<string>& modules_union(std::set<string>& modules, const std::set<string>& add) {
  for (std::set<string>::const_iterator it = add.begin(); it != add.end(); ++it) {
    modules.insert(*it);
  }
  return modules;
}

string t_go_generator::get_go_modules_used(t_type* type) {
  if (type->is_base_type() || type->is_container() || type->is_service()) {
    return "";
  }
  return get_real_go_module(type->get_program());
}

std::set<string> t_go_generator::get_go_modules_used(t_struct* strct) {
  const t_struct::members_type& members = strct->get_members();
  std::set<string> result;
  for (t_struct::members_type::const_iterator it = members.begin(); it != members.end(); ++it) {
    modules_union(result, get_go_modules_used((*it)->get_type()));
  }
  return result;
}

std::set<string> t_go_generator::get_go_modules_used(const std::vector<t_struct*>& structs) {
  std::set<string> result;
  for (std::vector<t_struct*>::const_iterator it = structs.begin(); it != structs.end(); ++it) {
    modules_union(result, get_go_modules_used(*it));
  }
  return result;
}

std::set<string> t_go_generator::get_go_modules_used(const std::vector<t_typedef*>& typedefs) {
  std::set<string> result;
  for (std::vector<t_typedef*>::const_iterator it = typedefs.begin(); it != typedefs.end(); ++it) {
    modules_union(result, get_go_modules_used((*it)->get_type()));
  }
  return result;
}

std::set<string> t_go_generator::get_go_modules_used(t_service* service, bool remote) {
  std::set<string> result;
  const std::vector<t_function*>& functions = service->get_functions();
  for (std::vector<t_function*>::const_iterator fn = functions.begin(); fn != functions.end(); ++fn) {
    if (!remote) modules_union(result, get_go_modules_used((*fn)->get_returntype()));
    modules_union(result, get_go_modules_used((*fn)->get_arglist()));
    if (!remote) modules_union(result, get_go_modules_used((*fn)->get_xceptions()));
  }
  return result;
}

/**
 * Prepares for file generation by opening up the necessary file output
 * streams.
 *
 * @param tprogram The program to generate
 */
void t_go_generator::init_generator() {
  includes_ = get_go_includes(program_);
  // Make output directory
  string module = get_real_go_module(program_);
  string target = module;
  package_dir_ = get_out_dir();
  while (true) {
    // TODO: Do better error checking here.
    MKDIR(package_dir_.c_str());
    if (module.empty()) {
      break;
    }
    string::size_type pos = module.find('.');
    if (pos == string::npos) {
      package_dir_ += "/";
      package_dir_ += module;
      package_name_ = module;
      module.clear();
    } else {
      package_dir_ += "/";
      package_dir_ += module.substr(0, pos);
      module.erase(0, pos+1);
    }
  }
  string::size_type loc;
  while((loc = target.find(".")) != string::npos) {
    target.replace(loc, 1, 1, '/');
  }
  
  // Make output file
  string f_types_name = package_dir_+"/"+"ttypes.go";
  f_types_.open(f_types_name.c_str());
  f_consts_ << "func init() {" << endl;
  
  vector<t_service*> services = program_->get_services();
  vector<t_service*>::iterator sv_iter;

  std::set<string> uses = get_go_modules_used(program_->get_typedefs());
  modules_union(uses, get_go_modules_used(program_->get_structs()));
  modules_union(uses, get_go_modules_used(program_->get_xceptions()));

  // Print header
  f_types_ <<
    go_autogen_comment() <<
    go_package() << 
    go_imports(true) <<
    render_includes(uses) <<
    render_fastbinary_includes() << endl << endl;
}

/**
 * Renders all the imports necessary for including another Thrift program
 */
string t_go_generator::render_includes(const std::set<string>& uses) {
  string result = "";
  for (std::set<string>::const_iterator it = uses.begin(); it != uses.end(); ++it) {
    if (includes_.find(*it) != includes_.end()) {
      result += "import \"thriftlib/" + *it + "\"\n";
    }
  }
  if (result.size() > 0) {
    result += "\n";
  }
  return result;
}

/**
 * Renders all the imports necessary to use the accelerated TBinaryProtocol
 */
string t_go_generator::render_fastbinary_includes() {
  return "";
}

/**
 * Autogen'd comment
 */
string t_go_generator::go_autogen_comment() {
  return
    std::string() +
	"/* Autogenerated by Thrift Compiler (" + THRIFT_VERSION + ")\n"
    " *\n"
    " * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING\n"
    " */\n";
}

/**
 * Prints standard thrift package
 */
string t_go_generator::go_package() {
  return
    string("package ") + package_name_ + ";\n\n";
}

/**
 * Prints standard thrift imports
 */
string t_go_generator::go_imports(bool types) {
  const t_program* program = get_program();
  const std::vector<t_struct *>& structs = program->get_structs();
  const std::vector<t_struct *>& xceptions = program->get_xceptions();
  if (!types || structs.size() > 0 || xceptions.size() > 0) {
    // We don't need to import thrift or fmt if we're generating type definitions
    // this program doesn't define any structs.
    return
      string("import (\n"
	     "        \"thrift\"\n"
	     //           "        \"strings\"\n"
	     "        \"fmt\"\n"
	     ")\n\n");
  }
  return "";
}

/**
 * Closes the type files
 */
void t_go_generator::close_generator() {
  // Close types file
  f_consts_ << "}" << endl;
  f_types_ << f_consts_.str() << endl;
  f_types_.close();
  f_consts_.clear();
}

/**
 * Generates a typedef. This is not done in go, types are all implicit.
 *
 * @param ttypedef The type definition
 */
void t_go_generator::generate_typedef(t_typedef* ttypedef) {
  
  generate_go_docstring(f_types_, ttypedef);
  string newTypeDef(publicize(ttypedef->get_symbolic()));
  string baseType(type_to_go_type(ttypedef->get_type()));
  if(baseType == newTypeDef)
    return;
  f_types_ <<
    "type " << newTypeDef << " " << baseType << endl << endl;
}

/**
 * Generates code for an enumerated type. Done using a class to scope
 * the values.
 *
 * @param tenum The enumeration
 */
void t_go_generator::generate_enum(t_enum* tenum) {
  std::ostringstream to_string_mapping, from_string_mapping;
  std::string tenum_name(publicize(tenum->get_name()));

  generate_go_docstring(f_types_, tenum);
  f_types_ <<
    "type " << tenum_name << " int" << endl <<
    "const (" << endl;
  
  to_string_mapping <<
    indent() << "func (p " << tenum_name << ") String() string {" << endl <<
    indent() << "  switch p {" << endl;
  from_string_mapping <<
    indent() << "func From" << tenum_name << "String(s string) " << tenum_name << " {" << endl <<
    indent() << "  switch s {" << endl;

  vector<t_enum_value*> constants = tenum->get_constants();
  vector<t_enum_value*>::iterator c_iter;
  int value = -1;
  for (c_iter = constants.begin(); c_iter != constants.end(); ++c_iter) {
    if ((*c_iter)->has_value()) {
      value = (*c_iter)->get_value();
    } else {
      ++value;
    }
    string iter_std_name(escape_string((*c_iter)->get_name()));
    string iter_name((*c_iter)->get_name());
    f_types_ <<
      indent() << "  " << iter_name << ' ' << tenum_name << " = " << value << endl;
    
    // Dictionaries to/from string names of enums
    to_string_mapping <<
      indent() << "  case " << iter_name << ": return \"" << iter_std_name << "\"" << endl;
    if(iter_std_name != escape_string(iter_name)) {
      from_string_mapping <<
        indent() << "  case \"" << iter_std_name << "\", \"" << escape_string(iter_name) << "\": return " << iter_name << endl;
    } else {
      from_string_mapping <<
        indent() << "  case \"" << iter_std_name << "\": return " << iter_name << endl;
    }
  }
  to_string_mapping <<
    indent() << "  }" << endl <<
    indent() << "  return \"\"" << endl <<
    indent() << "}" << endl;
  from_string_mapping <<
    indent() << "  }" << endl <<
    indent() << "  return " << tenum_name << "(-10000)" << endl <<
    indent() << "}" << endl;

  f_types_ <<
    indent() << ")" << endl <<
    to_string_mapping.str() << endl << from_string_mapping.str() << endl <<
    indent() << "func (p " << tenum_name << ") Value() int {" << endl <<
    indent() << "  return int(p)" << endl <<
    indent() << "}" << endl << endl <<
    indent() << "func (p " << tenum_name << ") IsEnum() bool {" << endl <<
    indent() << "  return true" << endl <<
    indent() << "}" << endl << endl;
}

/**
 * Generate a constant value
 */
void t_go_generator::generate_const(t_const* tconst) {
  t_type* type = tconst->get_type();
  string name = publicize(tconst->get_name());
  t_const_value* value = tconst->get_value();
  
  if(type->is_base_type() || type->is_enum()) {
    indent(f_types_) << "const " << name << " = " << render_const_value(type, value, name) << endl;
  } else {
    f_types_ <<
      indent() << "var " << name << " " << " " << type_to_go_type(type) << endl;
    f_consts_ <<
      "  " << name << " = " << render_const_value(type, value, name) << endl;
  }
}

/**
 * Prints the value of a constant with the given type. Note that type checking
 * is NOT performed in this function as it is always run beforehand using the
 * validate_types method in main.cc
 */
string t_go_generator::render_const_value(t_type* type, t_const_value* value, const string& name) {
  type = get_true_type(type);
  std::ostringstream out;

  if (type->is_base_type()) {
    t_base_type::t_base tbase = ((t_base_type*)type)->get_base();
    switch (tbase) {
    case t_base_type::TYPE_STRING:
      if (((t_base_type*)type)->is_binary()) {
        out << "[]byte(\"" << get_escaped_string(value) << "\")";
      } else {
        out << '"' << get_escaped_string(value) << '"';
      }
      break;
    case t_base_type::TYPE_BOOL:
      out << (value->get_integer() > 0 ? "true" : "false");
      break;
    case t_base_type::TYPE_BYTE:
    case t_base_type::TYPE_I16:
    case t_base_type::TYPE_I32:
    case t_base_type::TYPE_I64:
      out << value->get_integer();
      break;
    case t_base_type::TYPE_DOUBLE:
      if (value->get_type() == t_const_value::CV_INTEGER) {
        out << value->get_integer();
      } else {
        out << value->get_double();
      }
      break;
    default:
      throw "compiler error: no const of base type " + t_base_type::t_base_name(tbase);
    }
  } else if (type->is_enum()) {
    indent(out) << value->get_integer();
  } else if (type->is_struct() || type->is_xception()) {
    out << "Default" << publicize(type->get_name()) << endl <<
      indent() << "{" << endl;
    indent_up();
    const vector<t_field*>& fields = ((t_struct*)type)->get_members();
    vector<t_field*>::const_iterator f_iter;
    const map<t_const_value*, t_const_value*>& val = value->get_map();
    map<t_const_value*, t_const_value*>::const_iterator v_iter;
    for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
      t_type* field_type = NULL;
      for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
        if ((*f_iter)->get_name() == v_iter->first->get_string()) {
          field_type = (*f_iter)->get_type();
        }
      }
      if (field_type == NULL) {
        throw "type error: " + type->get_name() + " has no field " + v_iter->first->get_string();
      }
      if(field_type->is_base_type() || field_type->is_enum()) {
        out <<
          indent() << name << "." << publicize(v_iter->first->get_string()) << " = " << render_const_value(field_type, v_iter->second, name) << endl;
      } else {
        string k(tmp("k"));
        string v(tmp("v"));
        out <<
          indent() << v << " := " << render_const_value(field_type, v_iter->second, v) << endl <<
          indent() << name << "." << publicize(v_iter->first->get_string()) << " = " << v << endl;
      }
    }
    indent_down();
    out <<
      indent() << "}";
  } else if (type->is_map()) {
    t_type* ktype = ((t_map*)type)->get_key_type();
    t_type* vtype = ((t_map*)type)->get_val_type();
    const map<t_const_value*, t_const_value*>& val = value->get_map();
    out << "make(map[" << type_to_go_type(ktype, "") << "]"
	<< type_to_go_type(vtype, "") << ")" << endl <<
      indent() << "{" << endl;
    indent_up();
    map<t_const_value*, t_const_value*>::const_iterator v_iter;
    for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
      string k(tmp("k"));
      string v(tmp("v"));
      out <<
        indent() << k << " := " << render_const_value(ktype, v_iter->first, k) << endl <<
        indent() << v << " := " << render_const_value(vtype, v_iter->second, v) << endl <<
        indent() << name << "[" << k << "] = " << v << endl;
    }
    indent_down();
    out <<
      indent() << "}" << endl;
  } else if (type->is_list()) {
    t_type* etype = ((t_list*)type)->get_elem_type();
    const vector<t_const_value*>& val = value->get_list();
    out <<
      "make([]" << type_to_go_type(etype, "") << ", 0, " << val.size() << ")" << endl <<
      indent() << "{" << endl;
    indent_up();
    vector<t_const_value*>::const_iterator v_iter;
    for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
      string v(tmp("v"));
      out <<
        indent() << v << " := " << render_const_value(etype, *v_iter, v) << endl <<
        indent() << name << " = append(" << name << ", " << v << ")" << endl;
    }
    indent_down();
    out <<
      indent() << "}" << endl;
  } else if (type->is_set()) {
    t_type* etype = ((t_set*)type)->get_elem_type();
    const vector<t_const_value*>& val = value->get_list();
    out <<
      "make(map[" << type_to_go_type(etype, "") << "]thrift.Unit)" << endl <<
      indent() << "{" << endl;
    indent_up();
    vector<t_const_value*>::const_iterator v_iter;
    for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
      string v(tmp("v"));
      out <<
        indent() << v << " := " << render_const_value(etype, *v_iter, v) << endl <<
        indent() << name << "[" << v << "] = thrift.Set" << endl;
    }
    indent_down();
    out <<
      indent() << "}" << endl;
  } else {
    throw "CANNOT GENERATE CONSTANT FOR TYPE: " + type->get_name();
  }

  return out.str();
}

/**
 * Generates a go struct
 */
void t_go_generator::generate_struct(t_struct* tstruct) {
  generate_go_struct(tstruct, false);
}

/**
 * Generates a struct definition for a thrift exception. Basically the same
 * as a struct but extends the Exception class.
 *
 * @param txception The struct definition
 */
void t_go_generator::generate_xception(t_struct* txception) {
  generate_go_struct(txception, true);
}

/**
 * Generates a go struct
 */
void t_go_generator::generate_go_struct(t_struct* tstruct,
                                        bool is_exception) {
  generate_go_struct_definition(f_types_, tstruct, is_exception);
}

/**
 * Generates a struct definition for a thrift data type.
 *
 * @param tstruct The struct definition
 */
void t_go_generator::generate_go_struct_definition(ofstream& out,
                                                   t_struct* tstruct,
                                                   bool is_exception,
                                                   bool is_result) {

  const vector<t_field*>& members = tstruct->get_members();
  const vector<t_field*>& sorted_members = tstruct->get_sorted_members();
  vector<t_field*>::const_iterator m_iter;

  generate_go_docstring(out, tstruct);
  std::string tstruct_name(publicize(tstruct->get_name()));
  out << 
    indent() << "type " << tstruct_name << " struct {" << endl;

  /*
     Here we generate the structure specification for the fastbinary codec.
     These specifications have the following structure:
     thrift_spec -> tuple of item_spec
     item_spec -> nil | (tag, type_enum, name, spec_args, default)
     tag -> integer
     type_enum -> TType.I32 | TType.STRING | TType.STRUCT | ...
     name -> string_literal
     default -> nil  # Handled by __init__
     spec_args -> nil  # For simple types
                | (type_enum, spec_args)  # Value type for list/set
                | (type_enum, spec_args, type_enum, spec_args)
                  # Key and value for map
                | (class_name, spec_args_ptr) # For struct/exception
     class_name -> identifier  # Basically a pointer to the class
     spec_args_ptr -> expression  # just class_name.spec_args

     TODO(dreiss): Consider making this work for structs with negative tags.
  */

  // TODO(dreiss): Look into generating an empty tuple instead of nil
  // for structures with no members.
  // TODO(dreiss): Test encoding of structs where some inner structs
  // don't have thrift_spec.
  indent_up();
  if (sorted_members.empty() || (sorted_members[0]->get_key() >= 0)) {
    int sorted_keys_pos = 0;
    for (m_iter = sorted_members.begin(); m_iter != sorted_members.end(); ++m_iter) {
      for (; sorted_keys_pos != (*m_iter)->get_key(); sorted_keys_pos++) {
        if (sorted_keys_pos != 0) {
          indent(out) << "_ interface{}; // nil # " << sorted_keys_pos << endl;
        }
      }
      t_type* fieldType = (*m_iter)->get_type();
      string goType(type_to_go_type(fieldType));
      indent(out) << publicize(variable_name_to_go_name((*m_iter)->get_name())) << " " 
                  << goType << " `"
                  << "json:\"" << escape_string((*m_iter)->get_name())
		  << "\" thrift:\"" << escape_string((*m_iter)->get_name())
		  << ":" << (*m_iter)->get_key()
		  << ((*m_iter)->get_req() == t_field::T_REQUIRED
		      ? ",required"
		      : "")
		  << "\"`"
		  << "   // " << sorted_keys_pos
                  << endl;

      sorted_keys_pos ++;
    }
  } else {
    for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
      // This fills in default values, as opposed to nulls
      out <<
        indent() << publicize((*m_iter)->get_name()) << " " <<
                    type_to_enum((*m_iter)->get_type()) << endl;
    }
  }
  indent_down();
      
  string defaultVal = string("Default")+tstruct_name;

  out <<
    indent() << "}" << endl << endl <<
    indent() << "var " << defaultVal << " " << tstruct_name << endl << endl <<
    indent() << "func init() {" << endl;
  
  indent_up();
  for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
    // Initialize fields
    //t_type* type = (*m_iter)->get_type();
    string fieldName(publicize((*m_iter)->get_name()));
    string fullFieldName = defaultVal + "." + fieldName;
    if ((*m_iter)->get_value() != NULL) {
      out <<
        indent() << fullFieldName << " = " <<
        render_field_default_value(*m_iter, fullFieldName) << endl;
    }
  }
  out << indent() << "thrift.Register(\"" << tstruct->get_name() << "\", " 
      << defaultVal << ")" << endl;
  indent_down();
  out << indent() << "}" << endl << endl;

  out <<
    indent() << "func New" << tstruct_name << "() *" << tstruct_name << " {" << endl <<
    indent() << "  ret := Default" << tstruct_name << endl <<
    indent() << "  return &ret" << endl <<
    indent() << "}" << endl;

  // Printing utilities so that on the command line thrift
  // structs look pretty like dictionaries
  out << 
    indent() << "func (p *" << tstruct_name << ") TStructName() string {" << endl <<
    indent() << "  return \"" << escape_string(tstruct_name) << "\"" << endl <<
    indent() << "}" << endl << endl;
  
  out << 
    indent() << "func (p *" << tstruct_name << ") ThriftName() string {" << endl <<
    indent() << "  return \"" << escape_string(tstruct->get_name()) << "\"" << endl <<
    indent() << "}" << endl << endl;
  
  out <<
    indent() << "func (p *" << tstruct_name << ") String() string {" << endl <<
    indent() << "  if p == nil {" << endl <<
    indent() << "    return \"<nil>\"" << endl <<
    indent() << "  }" << endl <<
    indent() << "  return fmt.Sprintf(\"" << escape_string(tstruct_name) << "(%+v)\", *p)" << endl <<
    indent() << "}" << endl << endl;
}

/**
 * Generates a thrift service.
 *
 * @param tservice The service definition
 */
void t_go_generator::generate_service(t_service* tservice) {
  string f_service_name = package_dir_+"/"+service_name_+".go";
  f_service_.open(f_service_name.c_str());

  f_service_ <<
    go_autogen_comment() <<
    go_package() <<
    go_imports(false) <<
    render_includes(get_go_modules_used(tservice, false));

  if (tservice->get_extends() != NULL) {
    f_service_ <<
      "import \"thriftlib/" << get_real_go_module(tservice->get_extends()->get_program()) << "\"" << endl;
  }

  f_service_ <<
    //             "import (" << endl <<
    // indent() << "        \"os\"" << endl <<
    // indent() << ")" << endl << endl <<
    render_fastbinary_includes();

  f_service_ << endl;

  // Generate the three main parts of the service (well, two for now in PHP)
  generate_service_interface(tservice);
  generate_service_client(tservice);
  generate_service_server(tservice);
  generate_service_helpers(tservice);
  generate_service_remote(tservice);

  // Close service file
  f_service_ << endl;
  f_service_.close();
}

/**
 * Generates helper functions for a service.
 *
 * @param tservice The service to generate a header definition for
 */
void t_go_generator::generate_service_helpers(t_service* tservice) {
  vector<t_function*> functions = tservice->get_functions();
  vector<t_function*>::iterator f_iter;

  f_service_ <<
    "// HELPER FUNCTIONS AND STRUCTURES" << endl << endl;

  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    t_struct* ts = (*f_iter)->get_arglist();
    generate_go_struct_definition(f_service_, ts, false);
    generate_go_function_helpers(*f_iter);
  }
}

/**
 * Generates a struct and helpers for a function.
 *
 * @param tfunction The function
 */
void t_go_generator::generate_go_function_helpers(t_function* tfunction) {
  if (true || !tfunction->is_oneway()) {
    t_struct result(program_, tfunction->get_name() + "_result");
    t_field success(tfunction->get_returntype(), "success", 0);
    if (!tfunction->get_returntype()->is_void()) {
      result.append(&success);
    }

    t_struct* xs = tfunction->get_xceptions();
    const vector<t_field*>& fields = xs->get_members();
    vector<t_field*>::const_iterator f_iter;
    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
      result.append(*f_iter);
    }
    generate_go_struct_definition(f_service_, &result, false, true);
  }
}

/**
 * Generates a service interface definition.
 *
 * @param tservice The service to generate a header definition for
 */
void t_go_generator::generate_service_interface(t_service* tservice) {
  string extends = "";
  string extends_if = "";
  string serviceName(publicize(tservice->get_name()));
  string interfaceName = "I" + serviceName;
  if (tservice->get_extends() != NULL) {
    extends = type_name(tservice->get_extends());
    size_t index = extends.rfind(".");
    if(index != string::npos) {
      extends_if = "\n" + indent() + "  " + extends.substr(0, index + 1) + "I" + publicize(extends.substr(index + 1)) + "\n";
    } else {
      extends_if = "\n" + indent() + "I" + publicize(extends) + "\n";
    }
  }

  f_service_ <<
    indent() << "type " << interfaceName << " interface {" << extends_if;
  indent_up();
  generate_go_docstring(f_service_, tservice);
  vector<t_function*> functions = tservice->get_functions();
  if (!functions.empty()) {
    f_service_ << endl;
    vector<t_function*>::iterator f_iter;
    for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
      generate_go_docstring(f_service_, (*f_iter));
      f_service_ <<
        indent() << function_signature_if(*f_iter, "", true) << endl;
    }
  }

  indent_down();
  f_service_ <<
    indent() << "}" << endl << endl;
}

/**
 * Generates a service client definition.
 *
 * @param tservice The service to generate a server for.
 */
void t_go_generator::generate_service_client(t_service* tservice) {
  string extends = "";
  string extends_client = "";
  string extends_client_new = "";
  string serviceName(publicize(tservice->get_name()));
  if (tservice->get_extends() != NULL) {
    extends = type_name(tservice->get_extends());
    size_t index = extends.rfind(".");
    if(index != string::npos) {
      extends_client = extends.substr(0, index + 1) + publicize(extends.substr(index + 1)) + "Client";
      extends_client_new = extends.substr(0, index + 1) + "New" + publicize(extends.substr(index + 1)) + "Client";
    } else {
      extends_client = publicize(extends) + "Client";
      extends_client_new = "New" + extends_client;
    }
  }

  generate_go_docstring(f_service_, tservice);
  f_service_ <<
    indent() << "type " << serviceName << "Client struct {" << endl;
  indent_up();
  if(!extends_client.empty()) {
    f_service_ << 
      indent() << "*" << extends_client << endl;
  } else {
    f_service_ <<
      indent() << "Transport thrift.TTransport" << endl <<
      indent() << "ProtocolFactory thrift.TProtocolFactory" << endl <<
      indent() << "InputProtocol thrift.TProtocol" << endl <<
      indent() << "OutputProtocol thrift.TProtocol" << endl <<
      indent() << "SeqId int32" << endl /*<<
      indent() << "reqs map[int32]Deferred" << endl*/;
  }
  indent_down();
  f_service_ <<
    indent() << "}" << endl << endl;
  
  // Constructor function
  f_service_ <<
    indent() << "func New" << serviceName << "ClientFactory(t thrift.TTransport, f thrift.TProtocolFactory) *" << serviceName << "Client {" << endl;
  indent_up();
  f_service_ <<
    indent() << "return &" << serviceName << "Client";
  if (!extends.empty()) {
    f_service_ << 
      "{" << extends_client << ": " << extends_client_new << "Factory(t, f)}";
  } else {
    indent_up();
    f_service_ << "{Transport: t," << endl <<
      indent() << "ProtocolFactory: f," << endl <<
      indent() << "InputProtocol: f.GetProtocol(t)," << endl <<
      indent() << "OutputProtocol: f.GetProtocol(t)," << endl <<
      indent() << "SeqId: 0," << endl /*<<
      indent() << "Reqs: make(map[int32]Deferred)" << endl*/;
    indent_down();
    f_service_ <<
      indent() << "}" << endl;
  }
  indent_down();
  f_service_ <<
    indent() << "}" << endl << endl;
  
  
  // Constructor function
  f_service_ <<
    indent() << "func New" << serviceName << "ClientProtocol(t thrift.TTransport, iprot thrift.TProtocol, oprot thrift.TProtocol) *" << serviceName << "Client {" << endl;
  indent_up();
  f_service_ <<
    indent() << "return &" << serviceName << "Client";
  if (!extends.empty()) {
    f_service_ << 
      "{" << extends_client << ": " << extends_client_new << "Protocol(t, iprot, oprot)}" << endl;
  } else {
    indent_up();
    f_service_ << "{Transport: t," << endl <<
      indent() << "ProtocolFactory: nil," << endl <<
      indent() << "InputProtocol: iprot," << endl <<
      indent() << "OutputProtocol: oprot," << endl <<
      indent() << "SeqId: 0," << endl /*<<
      indent() << "Reqs: make(map[int32]interface{})" << endl*/;
    indent_down();
    f_service_ <<
      indent() << "}" << endl;
  }
  indent_down();
  f_service_ <<
    indent() << "}" << endl << endl;

  // Generate client method implementations
  vector<t_function*> functions = tservice->get_functions();
  vector<t_function*>::const_iterator f_iter;
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    t_struct* arg_struct = (*f_iter)->get_arglist();
    const vector<t_field*>& fields = arg_struct->get_members();
    vector<t_field*>::const_iterator fld_iter;
    string funname = publicize((*f_iter)->get_name());

    // Open function
    generate_go_docstring(f_service_, (*f_iter));
    f_service_ <<
      indent() << "func (p *" << serviceName << "Client) " << function_signature_if(*f_iter, "", true) << " {" << endl;
    indent_up();
    /*
    f_service_ <<
      indent() << "p.SeqId += 1" << endl;
    if (!(*f_iter)->is_oneway()) {
      f_service_ <<
        indent() << "d := defer.Deferred()" << endl <<
        indent() << "p.Reqs[p.SeqId] = d" << endl;
    }
    */
    f_service_ <<
      indent() << "err = p.Send" << funname << "(";

    bool first = true;
    for (fld_iter = fields.begin(); fld_iter != fields.end(); ++fld_iter) {
      if (first) {
        first = false;
      } else {
        f_service_ << ", ";
      }
      f_service_ << variable_name_to_go_name((*fld_iter)->get_name());
    }
    f_service_ << ")" << endl <<
      indent() << "if err != nil { return }" << endl;
    

    if (!(*f_iter)->is_oneway()) {
      f_service_ << 
        indent() << "return p.Recv" << funname << "()" << endl;
    } else {
      f_service_ <<
        indent() << "return" << endl;
    }
    indent_down();
    f_service_ << 
      indent() << "}" << endl << endl <<
      indent() << "func (p *" << serviceName << "Client) Send" << function_signature(*f_iter) << "(err error) {" << endl;
    
    indent_up();
    
    std::string argsname = privatize((*f_iter)->get_name()) + "Args";
    
    // Serialize the request header
    string args(tmp("args"));
    f_service_ <<
      indent() << "oprot := p.OutputProtocol" << endl <<
      indent() << "if oprot != nil {" << endl <<
      indent() << "  oprot = p.ProtocolFactory.GetProtocol(p.Transport)" << endl <<
      indent() << "  p.OutputProtocol = oprot" << endl <<
      indent() << "}" << endl <<
      indent() << "p.SeqId++" << endl <<
      indent() << "oprot.WriteMessageBegin(\"" << (*f_iter)->get_name() << "\", thrift.CALL, p.SeqId)" << endl <<
      indent() << args << " := Default" << publicize(argsname) << endl;
    
    for (fld_iter = fields.begin(); fld_iter != fields.end(); ++fld_iter) {
      f_service_ <<
        indent() << args << "." << publicize(variable_name_to_go_name((*fld_iter)->get_name())) << " = " << variable_name_to_go_name((*fld_iter)->get_name()) << endl;
    }
    
    // Write to the stream
    f_service_ <<
      indent() << "err = thrift.Marshal(oprot, " << args << ")" << endl <<
      indent() << "oprot.WriteMessageEnd()" << endl <<
      indent() << "oprot.Transport().Flush()" << endl <<
      indent() << "return" << endl;
    indent_down();
    f_service_ <<
      indent() << "}" << endl << endl;
    
    if (true) { //!(*f_iter)->is_oneway() || true) {}
      std::string resultname = privatize((*f_iter)->get_name()) + "Result";
      // Open function
      f_service_ << endl <<
                    indent() << "func (p *" << serviceName << "Client) Recv" << publicize((*f_iter)->get_name()) <<
                    "() (";
      if(!(*f_iter)->get_returntype()->is_void()) {
        f_service_ <<
          "value " << type_to_go_type((*f_iter)->get_returntype()) << ", ";
      }
      t_struct* exceptions = (*f_iter)->get_xceptions();
      string errs = argument_list(exceptions);
      if(errs.size()) {
        f_service_ << errs << ", ";
      }
      f_service_ << 
        "err error) {" << endl;
      indent_up();

      // TODO(mcslee): Validate message reply here, seq ids etc.
      
      string result(tmp("result"));
      string error(tmp("error"));
      string error2(tmp("error"));
      
      f_service_ <<
        indent() << "iprot := p.InputProtocol" << endl <<
        indent() << "if iprot == nil {" << endl <<
        indent() << "  iprot = p.ProtocolFactory.GetProtocol(p.Transport)" << endl <<
        indent() << "  p.InputProtocol = iprot" << endl <<
        indent() << "}" << endl <<
        indent() << "_, mTypeId, seqId, err := iprot.ReadMessageBegin()" << endl <<
        indent() << "if err != nil {" << endl <<
        indent() << "  return" << endl <<
        indent() << "}" << endl <<
        indent() << "if mTypeId == thrift.EXCEPTION {" << endl <<
        indent() << "  " << error << " := thrift.NewTApplicationExceptionDefault()" << endl <<
        indent() << "  var " << error2 << " error" << endl <<
        indent() << "  " << error2 << ", err = " << error << ".Read(iprot)" << endl <<
        indent() << "  if err != nil {" << endl <<
        indent() << "    return" << endl <<
        indent() << "  }" << endl <<
        indent() << "  if err = iprot.ReadMessageEnd(); err != nil {" << endl <<
        indent() << "    return" << endl <<
        indent() << "  }" << endl <<
        indent() << "  err = " << error2 << endl <<
        indent() << "  return" << endl <<
        indent() << "}" << endl <<
        indent() << "if p.SeqId != seqId {" << endl <<
        indent() << "  err = thrift.NewTApplicationException(thrift.BAD_SEQUENCE_ID, \"ping failed: out of sequence response\")" << endl <<
        indent() << "  return" << endl <<
        indent() << "}" << endl <<
        indent() << result << " := Default" << publicize(resultname)  << endl <<
        indent() << "err = thrift.Unmarshal(iprot, &" << result << ")" << endl <<
        indent() << "iprot.ReadMessageEnd()" << endl;

      // Careful, only return _result if not a void function
      if (!(*f_iter)->get_returntype()->is_void()) {
        f_service_ <<
          indent() << "value = " << result << ".Success" << endl;
      }
      
      t_struct* xs = (*f_iter)->get_xceptions();
      const std::vector<t_field*>& xceptions = xs->get_members();
      vector<t_field*>::const_iterator x_iter;
      for (x_iter = xceptions.begin(); x_iter != xceptions.end(); ++x_iter) {
        f_service_ <<
          indent() << "if " << result << "." << publicize(variable_name_to_go_name((*x_iter)->get_name())) << " != nil {" << endl <<
          indent() << "  " << variable_name_to_go_name((*x_iter)->get_name()) << " = " << result << "." << publicize(variable_name_to_go_name((*x_iter)->get_name())) << endl <<
          indent() << "}" << endl;
      }
      
      f_service_ <<
        indent() << "return" << endl;
      // Close function
      indent_down();
      f_service_ <<
        indent() << "}" << endl << endl;
    }
  }

  //indent_down();
  f_service_ <<
    endl;
}

/**
 * Manage a program pointer -- used to make sure that we emit package
 * prefixes when emitting the remote utilities.
 */
struct program_manager {
  t_program* prog;
  t_program** pprog;

  program_manager(t_program** p) : pprog(p) {
    prog = *p;
    *p = NULL;
  }

  ~program_manager() {
    *pprog = prog;
  }
};

/**
 * Generates a command line tool for making remote requests
 *
 * @param tservice The service to generate a remote for.
 */
void t_go_generator::generate_service_remote(t_service* tservice) {
  vector<t_function*> functions = tservice->get_functions();
  vector<t_function*>::iterator f_iter;

  string f_remote_dir = package_dir_+"/"+service_name_;
  MKDIR(f_remote_dir.c_str());
  string f_remote_name = f_remote_dir+"/"+service_name_+"-remote.go";
  ofstream f_remote;
  f_remote.open(f_remote_name.c_str());
  string service_module = get_real_go_module(program_);
  string::size_type loc;
  while((loc = service_module.find(".")) != string::npos) {
    service_module.replace(loc, 1, 1, '/');
  }

  program_manager mgr(&program_);

  // Figure out if we need to include strconv -- only if one or more service
  // params have an enum or numerical type.
  bool need_strconv = false;
  for (f_iter = functions.begin(); !need_strconv && f_iter != functions.end(); ++f_iter) {
    t_struct* arg_struct = (*f_iter)->get_arglist();
    const std::vector<t_field*>& args = arg_struct->get_members();
    vector<t_field*>::const_iterator a_iter;
    int num_args = args.size();
    for (int i = 0; !need_strconv && i < num_args; ++i) {
      t_type* the_type(args[i]->get_type());
      t_type* the_type2(get_true_type(the_type));
      if(the_type2->is_enum()) {
	need_strconv = true;
	break;
      } else if(the_type2->is_base_type()) {
        t_base_type::t_base e = ((t_base_type*)the_type2)->get_base();
        switch(e) {
	case t_base_type::TYPE_BYTE:
	case t_base_type::TYPE_I16:
	case t_base_type::TYPE_I32:
	case t_base_type::TYPE_I64:
	case t_base_type::TYPE_DOUBLE:
	  need_strconv = true;
	  break;
	case t_base_type::TYPE_VOID:
	case t_base_type::TYPE_STRING:
	case t_base_type::TYPE_BOOL:
	default:
	  break;
        }
      }
    }
  }
  
  f_remote <<
    go_autogen_comment() <<
    indent() << "package main" << endl << endl <<
    indent() << "import (" << endl <<
    indent() << "        \"flag\"" << endl <<
    indent() << "        \"fmt\"" << endl <<
    indent() << "        \"encoding/json\"" << endl <<
    indent() << "        \"net\"" << endl <<
    indent() << "        \"net/url\"" << endl <<
    indent() << "        \"os\"" << endl <<
    indent() << "        \"thrift\"" << endl <<
    indent() << "        \"thriftlib/" << service_module << "\"" << endl <<
    indent() << ")" << endl << endl;
  
  if (need_strconv) {
    f_remote <<
      indent() << "import \"strconv\"" << endl << endl;
  }
   
  f_remote <<
    render_includes(get_go_modules_used(tservice, true)) << endl <<
    indent() << endl <<
    indent() << "func Usage() {" << endl <<
    indent() << "  fmt.Fprint(os.Stderr, \"Usage of \", os.Args[0], \" [-h host:port] [-u url] [-f[ramed]] function [arg1 [arg2...]]:\\n\")" << endl <<
    indent() << "  flag.PrintDefaults()" << endl <<
    indent() << "  fmt.Fprint(os.Stderr, \"Functions:\\n\")" << endl;
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    string funcName((*f_iter)->get_name());
    string funcSignature(function_signature_if(*f_iter, "", true));
    f_remote <<
      indent() << "  fmt.Fprint(os.Stderr, \"  " << funcName << funcSignature.substr(funcSignature.find("(")) << "\\n\")" << endl;
  }
  f_remote <<
    indent() << "  fmt.Fprint(os.Stderr, \"\\n\")" << endl <<
    indent() << "  os.Exit(0)" << endl <<
    indent() << "}" << endl <<
    indent() << endl <<
    indent() << "func JsonPrint(args ...interface{}) {" << endl <<
    indent() << "  for _, arg := range args {" << endl <<
    indent() << "    switch arg.(type) {" << endl <<
    indent() << "    case error, thrift.TException:" << endl <<
    indent() << "      fmt.Print(arg)" << endl <<
    indent() << "    default:" << endl <<
    indent() << "      pp, err := json.Marshal(arg)" << endl <<
    indent() << "      if err != nil {" << endl <<
    indent() << "        fmt.Print(\"error marshalling arg: \")" << endl <<
    indent() << "        fmt.Print(arg)" << endl <<
    indent() << "      } else {" << endl <<
    indent() << "        fmt.Print(string(pp))" << endl <<
    indent() << "      }" << endl <<
    indent() << "    }" << endl <<
    indent() << "    fmt.Print(\"  \")" << endl <<
    indent() << "  }" << endl <<
    indent() << "  return" << endl <<
    indent() << "}" << endl <<
    indent() << endl <<
    indent() << "func main() {" << endl;
  indent_up();
  f_remote <<
    indent() << "flag.Usage = Usage" << endl <<
    indent() << "var host string" << endl <<
    indent() << "var port int" << endl <<
    indent() << "var protocol string" << endl <<
    indent() << "var urlString string" << endl <<
    indent() << "var framed bool" << endl <<
    indent() << "var useHttp bool" << endl <<
    indent() << "var help bool" << endl <<
    indent() << "var parsedUrl url.URL" << endl <<
    indent() << "var trans thrift.TTransport" << endl <<
    indent() << "flag.Usage = Usage" << endl <<
    indent() << "flag.StringVar(&host, \"h\", \"localhost\", \"Specify host and port\")" << endl <<
    indent() << "flag.IntVar(&port, \"p\", 9090, \"Specify port\")" << endl <<
    indent() << "flag.StringVar(&protocol, \"P\", \"binary\", \"Specify the protocol (binary, compact, simplejson, json)\")" << endl <<
    indent() << "flag.StringVar(&urlString, \"u\", \"\", \"Specify the url\")" << endl <<
    indent() << "flag.BoolVar(&framed, \"framed\", false, \"Use framed transport\")" << endl <<
    indent() << "flag.BoolVar(&useHttp, \"http\", false, \"Use http\")" << endl <<
    indent() << "flag.BoolVar(&help, \"help\", false, \"See usage string\")" << endl <<
    indent() << "flag.Parse()" << endl <<
    indent() << "if help || flag.NArg() == 0 {" << endl <<
    indent() << "  flag.Usage()" << endl <<
    indent() << "}" << endl <<
    indent() << endl <<
    indent() << "if len(urlString) > 0 {" << endl <<
    indent() << "  parsedUrl, err := url.Parse(urlString)" << endl <<
    indent() << "  if err != nil {" << endl <<
    indent() << "    fmt.Fprint(os.Stderr, \"Error parsing URL: \", err.Error(), \"\\n\")" << endl <<
    indent() << "    flag.Usage()" << endl <<
    indent() << "  }" << endl <<
    indent() << "  host = parsedUrl.Host" << endl <<
    //indent() << "  if len(parsedUrl.Port) == 0 { parsedUrl.Port = \"80\"; }" << endl <<
    //indent() << "  port = int(parsedUrl.Port)" << endl <<
    indent() << "  useHttp = len(parsedUrl.Scheme) <= 0 || parsedUrl.Scheme == \"http\"" << endl <<
    indent() << "} else if useHttp {" << endl <<
    indent() << "  _, err := url.Parse(fmt.Sprint(\"http://\", host, \":\", port))" << endl <<
    indent() << "  if err != nil {" << endl <<
    indent() << "    fmt.Fprint(os.Stderr, \"Error parsing URL: \", err.Error(), \"\\n\")" << endl <<
    indent() << "    flag.Usage()" << endl <<
    indent() << "  }" << endl <<
    indent() << "}" << endl <<
    indent() << endl <<
    indent() << "cmd := flag.Arg(0)" << endl <<
    indent() << "var err error" << endl <<
    indent() << "if useHttp {" << endl <<
    indent() << "  trans, err = thrift.NewTHttpClient(parsedUrl.String())" << endl <<
    indent() << "} else {" << endl <<
    indent() << "  addr, err := net.ResolveTCPAddr(\"tcp\", fmt.Sprint(host, \":\", port))" << endl <<
    indent() << "  if err != nil {" << endl <<
    indent() << "    fmt.Fprint(os.Stderr, \"Error resolving address\", err.Error())" << endl <<
    indent() << "    os.Exit(1)" << endl <<
    indent() << "  }" << endl <<
    indent() << "  trans, err = thrift.NewTNonblockingSocketAddr(addr)" << endl <<
    indent() << "  if framed {" << endl <<
    indent() << "    trans = thrift.NewTFramedTransport(trans)" << endl <<
    indent() << "  }" << endl <<
    indent() << "}" << endl <<
    indent() << "if err != nil {" << endl <<
    indent() << "  fmt.Fprint(os.Stderr, \"Error creating transport\", err.Error())" << endl <<
    indent() << "  os.Exit(1)" << endl <<
    indent() << "}" << endl <<
    indent() << "defer trans.Close()" << endl <<
    indent() << "var protocolFactory thrift.TProtocolFactory" << endl <<
    indent() << "switch protocol {" << endl <<
    indent() << "case \"compact\":" << endl <<
    indent() << "  protocolFactory = thrift.NewTCompactProtocolFactory()" << endl <<
    indent() << "  break" << endl <<
    indent() << "case \"simplejson\":" << endl <<
    indent() << "  protocolFactory = thrift.NewTSimpleJSONProtocolFactory()" << endl <<
    indent() << "  break" << endl <<
    indent() << "case \"json\":" << endl <<
    indent() << "  protocolFactory = thrift.NewTJSONProtocolFactory()" << endl <<
    indent() << "  break" << endl <<
    indent() << "case \"binary\", \"\":" << endl <<
    indent() << "  protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()" << endl <<
    indent() << "  break" << endl <<
    indent() << "default:" << endl <<
    indent() << "  fmt.Fprint(os.Stderr, \"Invalid protocol specified: \", protocol, \"\\n\")" << endl <<
    indent() << "  Usage()" << endl <<
    indent() << "  os.Exit(1)" << endl <<
    indent() << "}" << endl <<
    indent() << "client := " << package_name_ << ".New" << publicize(service_name_) << "ClientFactory(trans, protocolFactory)" << endl <<
    indent() << "if err = trans.Open(); err != nil {" << endl <<
    indent() << "  fmt.Fprint(os.Stderr, \"Error opening socket to \", host, \":\", port, \" \", err.Error())" << endl <<
    indent() << "  os.Exit(1)" << endl <<
    indent() << "}" << endl <<
    indent() << endl <<
    indent() << "switch cmd {" << endl;
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {

    t_struct* arg_struct = (*f_iter)->get_arglist();
    const std::vector<t_field*>& args = arg_struct->get_members();
    vector<t_field*>::const_iterator a_iter;
    int num_args = args.size();
    string funcName((*f_iter)->get_name());
    string pubName(publicize(funcName));
    
    f_remote <<
      indent() << "case \"" << escape_string(funcName) << "\":" << endl;
    indent_up();
    f_remote <<
      indent() << "if flag.NArg() - 1 != " << num_args << " {" << endl <<
      indent() << "  fmt.Fprint(os.Stderr, \"" << escape_string(pubName) << " requires " << num_args << " args\\n\")" << endl <<
      indent() << "  flag.Usage()" << endl <<
      indent() << "}" << endl;
    for (int i = 0; i < num_args; ++i) {
      int flagArg = i + 1;
      t_type* the_type(args[i]->get_type());
      t_type* the_type2(get_true_type(the_type));

      if(the_type2->is_enum()) {
	std::string module(get_real_go_module(the_type2->get_program()));
        f_remote << 
          indent() << "tmp" << i << ", err := (strconv.Atoi(flag.Arg(" << flagArg << ")))" << endl <<
          indent() << "if err != nil {" << endl <<
          indent() << "  Usage()" << endl <<
          indent() << " return" << endl <<
          indent() << "}" << endl <<
	  //          indent() << "argvalue" << i << " := " << package_name_ << "." << publicize(the_type->get_name()) << "(tmp" << i << ")" << endl;
          indent() << "argvalue" << i << " := " << module << "." << publicize(the_type->get_name()) << "(tmp" << i << ")" << endl;
      } else if(the_type2->is_base_type()) {
        t_base_type::t_base e = ((t_base_type*)the_type2)->get_base();
        string err(tmp("err"));
        switch(e) {
          case t_base_type::TYPE_VOID: break;
          case t_base_type::TYPE_STRING:
            f_remote << 
              indent() << "argvalue" << i << " := flag.Arg(" << flagArg << ")" << endl;
            break;
          case t_base_type::TYPE_BOOL:
            f_remote << 
              indent() << "argvalue" << i << " := flag.Arg(" << flagArg << ") == \"true\"" << endl;
            break;
          case t_base_type::TYPE_BYTE:
            f_remote << 
              indent() << "tmp" << i << ", " << err << " := (strconv.Atoi(flag.Arg(" << flagArg << ")))" << endl <<
              indent() << "if " << err << " != nil {" << endl <<
              indent() << "  Usage()" << endl <<
              indent() << "  return" << endl <<
              indent() << "}" << endl <<
              indent() << "argvalue" << i << " := byte(tmp" << i << ")" << endl;
            break;
          case t_base_type::TYPE_I16:
            f_remote << 
              indent() << "tmp" << i << ", " << err << " := (strconv.Atoi(flag.Arg(" << flagArg << ")))" << endl <<
              indent() << "if " << err << " != nil {" << endl <<
              indent() << "  Usage()" << endl <<
              indent() << "  return" << endl <<
              indent() << "}" << endl <<
              indent() << "argvalue" << i << " := byte(tmp" << i << ")" << endl;
            break;
          case t_base_type::TYPE_I32:
            f_remote << 
              indent() << "tmp" << i << ", " << err << " := (strconv.Atoi(flag.Arg(" << flagArg << ")))" << endl <<
              indent() << "if " << err << " != nil {" << endl <<
              indent() << "  Usage()" << endl <<
              indent() << "  return" << endl <<
              indent() << "}" << endl <<
              indent() << "argvalue" << i << " := int32(tmp" << i << ")" << endl;
            break;
          case t_base_type::TYPE_I64:
            f_remote <<
              indent() << "argvalue" << i << ", " << err << " := (strconv.ParseInt(flag.Arg(" << flagArg << "), 10, 64))" << endl <<
              indent() << "if " << err << " != nil {" << endl <<
              indent() << "  Usage()" << endl <<
              indent() << "  return" << endl <<
              indent() << "}" << endl;
            break;
          case t_base_type::TYPE_DOUBLE:
            f_remote <<
              indent() << "argvalue" << i << ", " << err << " := (strconv.ParseFloat(flag.Arg(" << flagArg << "), 64))" << endl <<
              indent() << "if " << err << " != nil {" << endl <<
              indent() << "  Usage()" << endl <<
              indent() << "  return" << endl <<
              indent() << "}" << endl;
            break;
          default:
            throw("Invalid base type in generate_service_remote");
        }
        //f_remote << publicize(args[i]->get_name()) << "(strconv.Atoi(flag.Arg(" << flagArg << ")))";
      } else if(the_type2->is_struct()) {
        string arg(tmp("arg"));
        string err1(tmp("err"));
	std::string prefix(go_type_prefix(the_type2));
        std::string tstruct_name(publicize(the_type2->get_name()));
        f_remote <<
	  indent() << arg << " := flag.Arg(" << flagArg << ")" << endl <<
	  indent() << "argvalue" << i << " := " << prefix << "New" << tstruct_name << "()" << endl <<
	  indent() << err1 << " := json.Unmarshal([]byte("<< arg << "), argvalue" << i <<")" << endl <<
	  indent() << "if " << err1 << " != nil {" << endl <<
	  indent() << "  fmt.Print(\"error parsing arg " << flagArg << "\")" << endl <<
	  indent() << "  fmt.Print(" << err1 << ")" << endl <<
	  indent() << "  return" << endl <<
	  indent() << "}" << endl;
      } else if(the_type2->is_list()) {
	string arg(tmp("arg"));
	string err1(tmp("err"));
	t_type* etype = ((t_list*)the_type2)->get_elem_type();
	std::string elttype(type_to_go_type(etype, ""));
	f_remote <<
	  indent() << arg << " := flag.Arg(" << flagArg << ")" << endl <<
	  indent() << "argvalue" << i << " := make([]" << elttype << ", 0)" << endl <<
	  indent() << err1 << " := json.Unmarshal([]byte("<< arg << "), &argvalue" << i << ")" << endl <<
	  indent() << "if " << err1 << " != nil {" << endl <<
	  indent() << "  fmt.Print(\"error parsing arg " << flagArg << "\")" << endl <<
	  indent() << "  fmt.Print(" << err1 << ")" << endl <<
	  indent() << "  return" << endl <<
	  indent() << "}" << endl;
      } else if (the_type2->is_map()) {
	string arg(tmp("arg"));
	string err1(tmp("err"));
	std::string ktype(type_to_go_type(((t_map*)the_type2)->get_key_type(), ""));
	std::string vtype(type_to_go_type(((t_map*)the_type2)->get_val_type(), ""));
	f_remote <<
	  indent() << arg << " : flag.Arg(" << flagArg << ")" << endl <<
	  indent() << "argvalue" << i << " := make(map[" << ktype << "]" << vtype << ")" << endl <<
	  indent() << err1 << " := json.Unmarshal([]byte(" << arg << "), &argvalue" << i << ")" << endl <<
	  indent() << "if " << err1 << " != nil {" << endl <<
	  indent() << "  fmt.Print(\"error parsing arg " << flagArg << "\")" << endl <<
	  indent() << "  fmt.Print(" << err1 << ")" << endl <<
	  indent() << "  return" << endl <<
	  indent() << "}" << endl;
      } else if (the_type2->is_set()) {
	string arg(tmp("arg"));
	string err1(tmp("err"));
	std::string ktype(type_to_go_type(((t_set*)the_type2)->get_elem_type(), ""));
	f_remote <<
	  indent() << arg << " : flag.Arg(" << flagArg << ")" << endl <<
	  indent() << "argvalue" << i << " := make(map[" << ktype << "]thrift.Unit)" << endl <<
	  indent() << err1 << " := json.Unmarshal([]byte(" << arg << "), &argvalue" << i << ")" << endl <<
	  indent() << "if " << err1 << " != nil {" << endl <<
	  indent() << "  fmt.Print(\"error parsing arg " << flagArg << "\")" << endl <<
	  indent() << "  fmt.Print(" << err1 << ")" << endl <<
	  indent() << "  return" << endl <<
	  indent() << "}" << endl;
      } else {
        string err1(tmp("err"));
        f_remote <<
          indent() << "argvalue" << i << ", " << err1 << " := eval(flag.Arg(" << flagArg << "))" << endl <<
          indent() << "if " << err1 << " != nil {" << endl <<
          indent() << "  Usage()" << endl <<
          indent() << "  return" << endl <<
          indent() << "}" << endl;
	std::ostringstream msg;
	msg << "Invalid argument type at " << i 
	    << " in generate_service_remote for " << funcName;
	throw(msg.str());
      }
      if(the_type->is_typedef()) {
        f_remote <<
          //indent() << "value" << i << " := " << package_name_ << "." << publicize(the_type->get_name()) << "(argvalue" << i << ")" << endl;
	  indent() << "value" << i << " := " << get_real_go_module(the_type->get_program()) << "." << publicize(the_type->get_name()) << "(argvalue" << i << ")" << endl;
      } else {
        f_remote <<
          indent() << "value" << i << " := argvalue" << i << endl;
      }
    }

    f_remote <<
      indent() << "JsonPrint(client." << pubName << "(";
    bool argFirst = true;
    for (int i = 0; i < num_args; ++i) {
      if (argFirst) {
        argFirst = false;
      } else {
        f_remote << ", ";
      }
      if(args[i]->get_type()->is_enum()) {
        f_remote << "value" << i;
      } else if(args[i]->get_type()->is_base_type()) {
        t_base_type::t_base e = ((t_base_type*)(args[i]->get_type()))->get_base();
        switch(e) {
          case t_base_type::TYPE_VOID: break;
          case t_base_type::TYPE_STRING:
          case t_base_type::TYPE_BOOL:
          case t_base_type::TYPE_BYTE:
          case t_base_type::TYPE_I16:
          case t_base_type::TYPE_I32:
          case t_base_type::TYPE_I64:
          case t_base_type::TYPE_DOUBLE:
            f_remote << "value" << i;
            break;
          default:
            throw("Invalid base type in generate_service_remote");
        }
        //f_remote << publicize(args[i]->get_name()) << "(strconv.Atoi(flag.Arg(" << flagArg << ")))";
      } else {
        f_remote << "value" << i;
      }
    }
    f_remote <<
      ")...)" << endl <<
      indent() << "fmt.Print(\"\\n\")" << endl <<
      indent() << "break" << endl;
    indent_down();
  }
  f_remote <<
    indent() << "case \"\":" << endl <<
    indent() << "  Usage()" << endl <<
    indent() << "  break" << endl <<
    indent() << "default:" << endl <<
    indent() << "  fmt.Fprint(os.Stderr, \"Invalid function \", cmd, \"\\n\")" << endl <<
    indent() << "}" << endl;
  indent_down();
  
  f_remote << 
    indent() << "}" << endl;
  

  // Close service file
  f_remote.close();
}

/**
 * Generates a service server definition.
 *
 * @param tservice The service to generate a server for.
 */
void t_go_generator::generate_service_server(t_service* tservice) {
  // Generate the dispatch methods
  vector<t_function*> functions = tservice->get_functions();
  vector<t_function*>::iterator f_iter;

  string extends = "";
  string extends_processor = "";
  string extends_processor_new = "";
  string serviceName(publicize(tservice->get_name()));
  if (tservice->get_extends() != NULL) {
    extends = type_name(tservice->get_extends());
    size_t index = extends.rfind(".");
    if(index != string::npos) {
      extends_processor = extends.substr(0, index + 1) + publicize(extends.substr(index + 1)) + "Processor";
      extends_processor_new = extends.substr(0, index + 1) + "New" + publicize(extends.substr(index + 1)) + "Processor";
    } else {
      extends_processor = publicize(extends) + "Processor";
      extends_processor_new = "New" + extends_processor;
    }
  }
  string pServiceName(privatize(serviceName));

  // Generate the header portion
  string self(tmp("self"));
  if(extends_processor.empty()) {
    f_service_ <<
      indent() << "type " << serviceName << "Processor struct {" << endl <<
      indent() << "  handler I" << serviceName << endl <<
      indent() << "  processorMap map[string]thrift.TProcessorFunction" << endl <<
      indent() << "}" << endl << endl <<
      indent() << "func (p *" << serviceName << "Processor) Handler() I" << serviceName << " {" << endl <<
      indent() << "  return p.handler" << endl <<
      indent() << "}" << endl << endl <<
      indent() << "func (p *" << serviceName << "Processor) AddToProcessorMap(key string, processor thrift.TProcessorFunction) {" << endl <<
      indent() << "  p.processorMap[key] = processor" << endl <<
      indent() << "}" << endl << endl <<
      indent() << "func (p *" << serviceName << "Processor) GetProcessorFunction(key string) (processor thrift.TProcessorFunction, exists bool) {" << endl <<
      indent() << "  processor, exists = p.processorMap[key]" << endl <<
      indent() << "  return processor, exists" << endl <<
      indent() << "}" << endl << endl <<
      indent() << "func (p *" << serviceName << "Processor) ProcessorMap() map[string]thrift.TProcessorFunction {" << endl <<
      indent() << "  return p.processorMap" << endl <<
      indent() << "}" << endl << endl <<
      indent() << "func New" << serviceName << "Processor(handler I" << serviceName << ") *" << serviceName << "Processor {" << endl << endl <<
      indent() << "  " << self << " := &" << serviceName << "Processor{handler:handler, processorMap:make(map[string]thrift.TProcessorFunction)}" << endl;
    for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
      string escapedFuncName(escape_string((*f_iter)->get_name()));
      f_service_ <<
        indent() << "  " << self << ".processorMap[\"" << escapedFuncName << "\"] = &" << pServiceName << "Processor" << publicize((*f_iter)->get_name()) << "{handler:handler}" << endl;
    }
    string x(tmp("x"));
    f_service_ <<
      indent() << "return " << self << endl <<
      indent() << "}" << endl << endl <<
      indent() << "func (p *" << serviceName << "Processor) Process(iprot, oprot thrift.TProtocol) (success bool, err thrift.TException) {" << endl <<
      indent() << "  name, _, seqId, err := iprot.ReadMessageBegin()" << endl <<
      indent() << "  if err != nil { return }" << endl <<
      indent() << "  process, nameFound := p.GetProcessorFunction(name)" << endl <<
      indent() << "  if !nameFound || process == nil {" << endl <<
      indent() << "    iprot.Skip(thrift.STRUCT)" << endl <<
      indent() << "    iprot.ReadMessageEnd()" << endl <<
      indent() << "    " << x << " := thrift.NewTApplicationException(thrift.UNKNOWN_METHOD, \"Unknown function \" + name)" << endl <<
      indent() << "    oprot.WriteMessageBegin(name, thrift.EXCEPTION, seqId)" << endl <<
      indent() << "    " << x << ".Write(oprot)" << endl <<
      indent() << "    oprot.WriteMessageEnd()" << endl <<
      indent() << "    oprot.Transport().Flush()" << endl <<
      indent() << "    return false, " << x << endl <<
      indent() << "  }" << endl <<
      indent() << "  return process.Process(seqId, iprot, oprot)" << endl <<
      indent() << "}" << endl << endl;
  } else {
    f_service_ <<
      indent() << "type " << serviceName << "Processor struct {" << endl <<
      indent() << "  super *" << extends_processor << endl <<
      indent() << "}" << endl << endl <<
      indent() << "func (p *" << serviceName << "Processor) Handler() I" << serviceName << " {" << endl <<
      indent() << "  return p.super.Handler().(I" << serviceName << ")" << endl <<
      indent() << "}" << endl << endl <<
      indent() << "func (p *" << serviceName << "Processor) AddToProcessorMap(key string, processor thrift.TProcessorFunction) {" << endl <<
      indent() << "  p.super.AddToProcessorMap(key, processor)" << endl <<
      indent() << "}" << endl << endl <<
      indent() << "func (p *" << serviceName << "Processor) GetProcessorFunction(key string) (processor thrift.TProcessorFunction, exists bool) {" << endl <<
      indent() << "  return p.super.GetProcessorFunction(key)" << endl <<
      indent() << "}" << endl << endl <<
      indent() << "func (p *" << serviceName << "Processor) ProcessorMap() map[string]thrift.TProcessorFunction {" << endl <<
      indent() << "  return p.super.ProcessorMap()" << endl <<
      indent() << "}" << endl << endl <<
      indent() << "func New" << serviceName << "Processor(handler I" << serviceName << ") *" << serviceName << "Processor {" << endl <<
      indent() << "  " << self << " := &" << serviceName << "Processor{super: " << extends_processor_new << "(handler)}" << endl;
    for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
      string escapedFuncName(escape_string((*f_iter)->get_name()));
      f_service_ <<
        indent() << "  " << self << ".AddToProcessorMap(\"" << escapedFuncName << "\", &" << pServiceName << "Processor" << publicize((*f_iter)->get_name()) << "{handler:handler})" << endl;
    }
    f_service_ <<
      indent() << "  return " << self << endl <<
      indent() << "}" << endl << endl <<
      indent() << "func (p *" << serviceName << "Processor) Process(iprot, oprot thrift.TProtocol) (bool, thrift.TException) {" << endl <<
      indent() << "  return p.super.Process(iprot, oprot)" << endl <<
      indent() << "}" << endl << endl;
  }
  
  // Generate the process subfunctions
  for (f_iter = functions.begin(); f_iter != functions.end(); ++f_iter) {
    generate_process_function(tservice, *f_iter);
  }

  f_service_ << endl;
}

/**
 * Generates a process function definition.
 *
 * @param tfunction The function to write a dispatcher for
 */
void t_go_generator::generate_process_function(t_service* tservice,
                                               t_function* tfunction) {
  // Open function
  string processorName = privatize(tservice->get_name()) + "Processor" + publicize(tfunction->get_name());
  
  string argsname = publicize(tfunction->get_name()) + "Args";
  string resultname = publicize(tfunction->get_name()) + "Result";
  
  //t_struct* xs = tfunction->get_xceptions();
  //const std::vector<t_field*>& xceptions = xs->get_members();
  vector<t_field*>::const_iterator x_iter;
  
  f_service_ <<
    indent() << "type " << processorName << " struct {" << endl <<
    indent() << "  handler I" << publicize(tservice->get_name()) << endl <<
    indent() << "}" << endl << endl <<
    indent() << "func (p *" << processorName << ") Process(seqId int32, iprot, oprot thrift.TProtocol) (success bool, err thrift.TException) {" << endl;
  indent_up();
  
  f_service_ <<
    indent() << "args := Default" << argsname  << endl <<
    indent() << "if err = thrift.Unmarshal(iprot, &args); err != nil {" << endl <<
    indent() << "  iprot.ReadMessageEnd()" << endl <<
    indent() << "  x := thrift.NewTApplicationException(thrift.PROTOCOL_ERROR, err.Error())" << endl <<
    indent() << "  oprot.WriteMessageBegin(\"" << escape_string(tfunction->get_name()) << "\", thrift.EXCEPTION, seqId)" << endl <<
    indent() << "  x.Write(oprot)" << endl <<
    indent() << "  oprot.WriteMessageEnd()" << endl <<
    indent() << "  oprot.Transport().Flush()" << endl <<
    indent() << "  return" << endl <<
    indent() << "}" << endl <<
    indent() << "iprot.ReadMessageEnd()" << endl <<
    indent() << "result := Default" << resultname << endl <<
    indent() << "if ";
  if (!tfunction->is_oneway()) {
    if(!tfunction->get_returntype()->is_void()) {
      f_service_ << "result.Success, ";
    }
    t_struct* exceptions = tfunction->get_xceptions();
    const vector<t_field*>& fields = exceptions->get_members();
    vector<t_field*>::const_iterator f_iter;
    for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
      f_service_ << "result." << publicize(variable_name_to_go_name((*f_iter)->get_name())) << ", ";
    }
  }
  
  
  // Generate the function call
  t_struct* arg_struct = tfunction->get_arglist();
  const std::vector<t_field*>& fields = arg_struct->get_members();
  vector<t_field*>::const_iterator f_iter;

  f_service_ <<
    "err = p.handler." << publicize(tfunction->get_name()) << "(";
  bool first = true;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    if (first) {
      first = false;
    } else {
      f_service_ << ", ";
    }
    f_service_ << "args." << publicize(variable_name_to_go_name((*f_iter)->get_name()));
  }
  f_service_ << "); err != nil {" << endl <<
    indent() << "  x := thrift.NewTApplicationException(thrift.INTERNAL_ERROR, \"Internal error processing " << escape_string(tfunction->get_name()) << ": \" + err.Error())" << endl <<
    indent() << "  oprot.WriteMessageBegin(\"" << escape_string(tfunction->get_name()) << "\", thrift.EXCEPTION, seqId)" << endl <<
    indent() << "  x.Write(oprot)" << endl <<
    indent() << "  oprot.WriteMessageEnd()" << endl <<
    indent() << "  oprot.Transport().Flush()" << endl <<
    indent() << "  return" << endl <<
    indent() << "}" << endl <<
    indent() << "if err2 := oprot.WriteMessageBegin(\"" << escape_string(tfunction->get_name()) << "\", thrift.REPLY, seqId); err2 != nil {" << endl <<
    indent() << "  err = err2" << endl <<
    indent() << "}" << endl <<
    indent() << "if err2 := thrift.Marshal(oprot, result); err == nil && err2 != nil {" << endl <<
    indent() << "  err = err2" << endl <<
    indent() << "}" << endl <<
    indent() << "if err2 := oprot.WriteMessageEnd(); err == nil && err2 != nil {" << endl <<
    indent() << "  err = err2" << endl <<
    indent() << "}" << endl <<
    indent() << "if err2 := oprot.Transport().Flush(); err == nil && err2 != nil {" << endl <<
    indent() << "  err = err2" << endl <<
    indent() << "}" << endl <<
    indent() << "if err != nil {" << endl <<
    indent() << "  return" << endl <<
    indent() << "}" << endl <<
    indent() << "return true, err" << endl;
  indent_down();
  f_service_ <<
    indent() << "}" << endl << endl;
  /*
  indent(f_service_) <<
      "func (p *" << publicize(tservice->get_name()) << "Client) WriteResultsSuccess" << publicize(tfunction->get_name()) <<
      "(success bool, result " << publicize(tfunction->get_name()) << "Result, seqid int32, oprot thrift.TProtocol) (err error) {" << endl;
  indent_up();
  f_service_ <<
    indent() << "result.Success = success" << endl <<
    indent() << "oprot.WriteMessageBegin(\"" << escape_string(tfunction->get_name()) << "\", thrift.REPLY, seqid)" << endl <<
    indent() << "result.Write(oprot)" << endl <<
    indent() << "oprot.WriteMessageEnd()" << endl <<
    indent() << "oprot.Transport().Flush()" << endl <<
    indent() << "return" << endl;
  indent_down();
  f_service_ << 
    indent() << "}" << endl << endl;
  */
  // Try block for a function with exceptions
  /*
  if (!tfunction->is_oneway() && xceptions.size() > 0) {
    indent(f_service_) <<
      "func (p *" << publicize(tservice->get_name()) << "Client) WriteResultsException" << publicize(tfunction->get_name()) <<
      "(error *" << publicize(tfunction->get_name()) << ", result *, seqid, oprot) (err error) {" << endl;
    indent_up();

    // Kinda absurd
    for (x_iter = xceptions.begin(); x_iter != xceptions.end(); ++x_iter) {
      f_service_ <<
        indent() << "except " << type_name((*x_iter)->get_type()) << ", " << (*x_iter)->get_name() << ":" << endl;
      if (!tfunction->is_oneway()) {
        indent_up();
        f_service_ <<
          indent() << "result." << (*x_iter)->get_name() << " = " << (*x_iter)->get_name() << endl;
        indent_down();
      } else {
        f_service_ <<
          indent() << "pass" << endl;
      }
    }
    f_service_ <<
      indent() << "err = oprot.WriteMessageBegin(\"" << escape_string(tfunction->get_name()) << "\", thrift.REPLY, seqid)" << endl <<
      indent() << "if err != nil { return err }" << endl <<
      indent() << "err = result.Write(oprot)" << endl <<
      indent() << "if err != nil { return err }" << endl <<
      indent() << "err = oprot.WriteMessageEnd()" << endl <<
      indent() << "if err != nil { return err }" << endl <<
      indent() << "err = oprot.Transport().Flush()" << endl <<
      indent() << "if err != nil { return err }" << endl;
    indent_down();
    f_service_ << "}" << endl << endl;
  }
  */
}

/**
 * Generates the docstring for a given struct.
 */
void t_go_generator::generate_go_docstring(ofstream& out,
                                               t_struct* tstruct) {
  generate_go_docstring(out, tstruct, tstruct, "Attributes");
}

/**
 * Generates the docstring for a given function.
 */
void t_go_generator::generate_go_docstring(ofstream& out,
                                               t_function* tfunction) {
  generate_go_docstring(out, tfunction, tfunction->get_arglist(), "Parameters");
}

/**
 * Generates the docstring for a struct or function.
 */
void t_go_generator::generate_go_docstring(ofstream& out,
                                               t_doc*    tdoc,
                                               t_struct* tstruct,
                                               const char* subheader) {
  bool has_doc = false;
  stringstream ss;
  if (tdoc->has_doc()) {
    has_doc = true;
    ss << tdoc->get_doc();
  }

  const vector<t_field*>& fields = tstruct->get_members();
  if (fields.size() > 0) {
    if (has_doc) {
      ss << endl;
    }
    has_doc = true;
    ss << subheader << ":\n";
    vector<t_field*>::const_iterator p_iter;
    for (p_iter = fields.begin(); p_iter != fields.end(); ++p_iter) {
      t_field* p = *p_iter;
      ss << " - " << publicize(variable_name_to_go_name(p->get_name()));
      if (p->has_doc()) {
        ss << ": " << p->get_doc();
      } else {
        ss << endl;
      }
    }
  }

  if (has_doc) {
    generate_docstring_comment(out,
      "/**\n",
      " * ", ss.str(),
      " */\n");
  }
}

/**
 * Generates the docstring for a generic object.
 */
void t_go_generator::generate_go_docstring(ofstream& out,
                                               t_doc* tdoc) {
  if (tdoc->has_doc()) {
    generate_docstring_comment(out,
      "/**\n",
      " *", tdoc->get_doc(),
      " */\n");
  }
}

/**
 * Declares an argument, which may include initialization as necessary.
 *
 * @param tfield The field
 */
string t_go_generator::declare_argument(t_field* tfield) {
  std::ostringstream result;
  result << publicize(tfield->get_name()) << "=";
  if (tfield->get_value() != NULL) {
    result << "thrift_spec[" <<
      tfield->get_key() << "][4]";
  } else {
    result << "nil";
  }
  return result.str();
}

/**
 * Renders a field default value, returns nil otherwise.
 *
 * @param tfield The field
 */
string t_go_generator::render_field_default_value(t_field* tfield, const string& name) {
  t_type* type = get_true_type(tfield->get_type());
  if (tfield->get_value() != NULL) {
    return render_const_value(type, tfield->get_value(), name);
  } else {
    return "nil";
  }
}

/**
 * Renders a function signature of the form 'type name(args)'
 *
 * @param tfunction Function definition
 * @return String of rendered function definition
 */
string t_go_generator::function_signature(t_function* tfunction,
                                          string prefix) {
  // TODO(mcslee): Nitpicky, no ',' if argument_list is empty
  return
    publicize(prefix + tfunction->get_name()) + 
      "(" + argument_list(tfunction->get_arglist()) + ")";
}

/**
 * Renders an interface function signature of the form 'type name(args)'
 *
 * @param tfunction Function definition
 * @return String of rendered function definition
 */
string t_go_generator::function_signature_if(t_function* tfunction,
                                             string prefix,
                                             bool addOsError) {
  // TODO(mcslee): Nitpicky, no ',' if argument_list is empty
  string signature = publicize(prefix + tfunction->get_name()) + "(";
  signature += argument_list(tfunction->get_arglist()) + ") (";
  t_type* ret = tfunction->get_returntype();
  t_struct* exceptions = tfunction->get_xceptions();
  string errs = argument_list(exceptions);
  string retval(tmp("retval"));
  if(!ret->is_void()) {
    signature += retval + " " + type_to_go_type(ret);
    if(addOsError || errs.size()==0) {
      signature += ", ";
    }
  }
  if(errs.size()>0) {
    signature += errs;
    if(addOsError)
      signature += ", ";
  }
  if(addOsError) {
    signature += "err error";
  }
  signature += ")";
  return signature;
}


/**
 * Renders a field list
 */
string t_go_generator::argument_list(t_struct* tstruct) {
  string result = "";
  
  const vector<t_field*>& fields = tstruct->get_members();
  vector<t_field*>::const_iterator f_iter;
  bool first = true;
  for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
    if (first) {
      first = false;
    } else {
      result += ", ";
    }
    result += variable_name_to_go_name((*f_iter)->get_name()) + " " + type_to_go_type((*f_iter)->get_type());
  }
  return result;
}

 string t_go_generator::go_type_prefix(t_type* type) {
  t_program* program = type->get_program();
  if (type->is_service() || ((program != NULL) && (program != program_))) {
    return get_real_go_module(program) + ".";
  }
  return "";
}

string t_go_generator::type_name(t_type* ttype,
				 string (*filter)(const string&),
				 const string& prefix) {
  return go_type_prefix(ttype) + prefix + filter(ttype->get_name());
}

/**
 * Converts the parse type to a go tyoe
 */
string t_go_generator::type_to_enum(t_type* type) {
  type = get_true_type(type);

  if (type->is_base_type()) {
    t_base_type::t_base tbase = ((t_base_type*)type)->get_base();
    switch (tbase) {
    case t_base_type::TYPE_VOID:
      throw "NO T_VOID CONSTRUCT";
    case t_base_type::TYPE_STRING:
      if (((t_base_type*)type)->is_binary()) {
        return "thrift.BINARY";
      }
      return "thrift.STRING";
    case t_base_type::TYPE_BOOL:
      return "thrift.BOOL";
    case t_base_type::TYPE_BYTE:
      return "thrift.BYTE";
    case t_base_type::TYPE_I16:
      return "thrift.I16";
    case t_base_type::TYPE_I32:
      return "thrift.I32";
    case t_base_type::TYPE_I64:
      return "thrift.I64";
    case t_base_type::TYPE_DOUBLE:
      return "thrift.DOUBLE";
    }
  } else if (type->is_enum()) {
    return "thrift.I32";
  } else if (type->is_struct() || type->is_xception()) {
    return "thrift.STRUCT";
  } else if (type->is_map()) {
    return "thrift.MAP";
  } else if (type->is_set()) {
    return "thrift.SET";
  } else if (type->is_list()) {
    return "thrift.LIST";
  }

  throw "INVALID TYPE IN type_to_enum: " + type->get_name();
}

/**
 * Converts the parse type to a go tyoe
 */
string t_go_generator::type_to_go_type(t_type* type, std::string ptr) {
  //type = get_true_type(type);
  if (type->is_base_type()) {
    t_base_type::t_base tbase = ((t_base_type*)type)->get_base();
    switch (tbase) {
    case t_base_type::TYPE_VOID:
      throw "";
    case t_base_type::TYPE_STRING:
      if (((t_base_type*)type)->is_binary()) {
        return "[]byte";
      }
      return "string";
    case t_base_type::TYPE_BOOL:
      return "bool";
    case t_base_type::TYPE_BYTE:
      return "byte";
    case t_base_type::TYPE_I16:
      return "int16";
    case t_base_type::TYPE_I32:
      return "int32";
    case t_base_type::TYPE_I64:
      return "int64";
    case t_base_type::TYPE_DOUBLE:
      return "float64";
    }
  } else if (type->is_enum()) {
    return type_name(type, publicize);
  } else if (type->is_struct() || type->is_xception()) {
    return ptr + type_name(type, publicize);
  } else if (type->is_map()) {
    string keyType = type_to_go_type(((t_map*)type)->get_key_type(), "");
    string valueType = type_to_go_type(((t_map*)type)->get_val_type(), "");
    return string("map[") + keyType + "]" + valueType;
  } else if (type->is_set()) {
    string elemType = type_to_go_type(((t_set*)type)->get_elem_type(), "");
    return string("map[") + elemType + "]thrift.Unit";
  } else if (type->is_list()) {
    string elemType = type_to_go_type(((t_list*)type)->get_elem_type(), "");
    return string("[]") + elemType;
  } else if (type->is_typedef()) {
    return go_type_prefix(type) + publicize(((t_typedef*)type)->get_symbolic());
  }

  throw "INVALID TYPE IN type_to_go_type: " + type->get_name();
}


/**
 * Converts the parse type to a go tyoe
 */
bool t_go_generator::can_be_nil(t_type* type) {
  type = get_true_type(type);

  if (type->is_base_type()) {
    t_base_type::t_base tbase = ((t_base_type*)type)->get_base();
    switch (tbase) {
    case t_base_type::TYPE_VOID:
      throw "Invalid Type for can_be_nil";
    case t_base_type::TYPE_BOOL:
    case t_base_type::TYPE_BYTE:
    case t_base_type::TYPE_I16:
    case t_base_type::TYPE_I32:
    case t_base_type::TYPE_I64:
    case t_base_type::TYPE_DOUBLE:
      return false;
    case t_base_type::TYPE_STRING:
      return (((t_base_type*)type)->is_binary());
    }
  } else if (type->is_enum()) {
    return false;
  } else if (type->is_struct() || type->is_xception()) {
    return true;
  } else if (type->is_map()) {
    return true;
  } else if (type->is_set()) {
    return true;
  } else if (type->is_list()) {
    return true;
  }

  throw "INVALID TYPE IN can_be_nil: " + type->get_name();
}

THRIFT_REGISTER_GENERATOR(go, "Go", "");
